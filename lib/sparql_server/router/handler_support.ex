defmodule SparqlServer.Router.HandlerSupport do
  alias InterpreterTerms.SymbolMatch, as: Sym
  alias Updates.QueryAnalyzer
  alias QueryAnalyzer.Iri
  alias SparqlServer.Router.AccessGroupSupport, as: AccessGroupSupport
  alias QueryAnalyzer.Types.Quad, as: Quad
  alias Updates.QueryAnalyzer, as: QueryAnalyzer
  alias Updates.QueryAnalyzer.P, as: QueryAnalyzerProtocol

  require Logger
  require ALog

  @doc """
  Handles the processing of a query.  Calculating the response whilst
  possibly getting some contents from the connection.  The new
  connection (to which the response may be sent) is yielded back,
  together with the response which should be set on the client.
  """
  @spec handle_query(String.t(), SparqlClient.query_types(), Plug.Conn.t()) ::
          {Plug.Conn.t(), any}
  def handle_query(query, kind, conn) do
    Logging.EnvLog.log(:log_incoming_sparql_queries, "Incoming SPARQL query: #{query}")

    Logging.EnvLog.inspect(query, :inspect_incoming_sparql_queries, label: "Incoming SPARQL query")

    handle_query_with_worker(query, kind, conn)
  end

  @spec try_get_mu_request_timeout(Plug.Conn.t()) :: integer | nil
  defp try_get_mu_request_timeout(conn) do
    case Plug.Conn.get_req_header(conn, "mu-request-timeout") do
      [value | _] ->
        try do
          String.to_integer(value)
        rescue
          _ -> nil
        end

      _ ->
        nil
    end
  end

  @doc """
  Handles the query, like handle_query/3 but using a worker instead.
  This should speed up the execution.
  """
  def handle_query_with_worker(query, kind, conn) do
    # TODO: treat timeout more exactly by subtracting time needed for fetching worker
    timeout =
      try_get_mu_request_timeout(conn) ||
        Application.get_env(:"mu-authorization", :query_max_processing_time)

    :poolboy.transaction(
      :query_worker,
      fn pid ->
        try do
          GenServer.call(pid, {:handle_query, query, kind, conn}, timeout)
        catch
          :exit, {:timeout, call_info} ->
            Support.JobCancellation.cancel!(pid)
            exit({:timeout, call_info})
        end
      end,
      timeout
    )
  end

  @doc """
  Ensure the syntax is stored in the template local store.
  """
  def ensure_syntax_in_store(template_local_store) do
    if Map.has_key?(template_local_store, :sparql_syntax) do
      template_local_store
    else
      Map.put(template_local_store, :sparql_syntax, Parser.parse_sparql())
    end
  end

  defp maybe_cancel_job_for_testing do
    failure_rate = Application.get_env(:"mu-authorization", :testing_auth_query_error_rate)

    if is_float(failure_rate) and :rand.uniform() < failure_rate do
      IO.puts("Letting query fail from failure_rate")
      throw({:job_cancelled})
    end
  end

  def handle_query_with_template_local_store(query, kind, conn, template_local_store) do
    maybe_cancel_job_for_testing()

    top_level_key =
      case kind do
        :query -> :QueryUnit
        :update -> :UpdateUnit
        :any -> :Sparql
      end

    new_template_local_store = ensure_syntax_in_store(template_local_store)

    {parsed_form, new_template_local_store} =
      query
      |> ALog.di("Raw received query")
      |> String.trim()
      # TODO: check if this is valid and/or ensure parser skips \r between words.
      |> String.replace("\r", "")
      |> Parser.parse_query_full_local(top_level_key, new_template_local_store)

    parsed_form =
      parsed_form
      |> wrap_query_in_toplevel
      |> ALog.di("Wrapped parsed query")

    query_manipulator =
      if is_select_query(parsed_form) do
        &manipulate_select_query/2
      else
        &manipulate_update_query/2
      end

    case query_manipulator.(parsed_form, conn) do
      {conn, new_parsed_forms, post_processing} ->
        query_type =
          if Enum.any?(new_parsed_forms, fn q -> !is_select_query(q) end) do
            :read
          else
            :write
          end

        encoded_response =
          new_parsed_forms
          |> ALog.di("New parsed forms")
          |> Enum.map(&SparqlClient.execute_parsed(&1, request: conn, query_type: query_type))
          |> List.first()
          |> Poison.encode!()

        post_processing.()

        {conn, {200, encoded_response}, new_template_local_store}

      {:fail, reason} ->
        encoded_response_string = Poison.encode!(%{errors: [%{status: "403", title: reason}]})
        {conn, {403, encoded_response_string}, new_template_local_store}
    end
  end

  def wrap_query_in_toplevel(%Sym{symbol: :Sparql} = matched) do
    matched
  end

  def wrap_query_in_toplevel(%Sym{string: str} = matched) do
    # Only public for benchmark
    %Sym{
      symbol: :Sparql,
      string: str,
      submatches: [matched]
    }
  end

  @doc """
  Yields non-nil iff the query is a select query.
  """
  def is_select_query(query) do
    case query do
      %Sym{
        symbol: :Sparql,
        submatches: [%Sym{symbol: :QueryUnit}]
      } ->
        true

      _ ->
        false
    end
  end

  ### Manipulates the select query yielding back the valid set of
  ### queries which should be executed on the database.
  defp manipulate_select_query(query, %Plug.Conn{} = conn) do
    {conn, authorization_groups} = AccessGroupSupport.calculate_access_groups(conn)

    {conn, query} =
      if authorization_groups == :sudo do
        {conn, query}
      else
        query = manipulate_select_query(query, authorization_groups, :read)
        conn = AccessGroupSupport.put_access_groups(conn, authorization_groups)

        {conn, query}
      end

    {conn, [query], fn -> :ok end}
  end

  @doc """
  Updates a select query to cope with the supplied access rights
  """
  @spec manipulate_select_query(
          Parser.query(),
          SparqlServer.Router.AccessGroupSupport.decoded_json_access_groups(),
          SparqlClient.query_types()
        ) :: Parser.query()
  def manipulate_select_query(query, authorization_groups, useage) do
    if authorization_groups == :sudo do
      query
    else
      query
      |> Manipulators.SparqlQuery.remove_from_statements()
      |> Acl.process_query(Acl.UserGroups.for_use(useage), authorization_groups)
      |> elem(0)
    end
  end

  ## Create tuple from literal {type, value}
  defp get_result_tuple(x) do
    out = QueryAnalyzerProtocol.to_sparql_result_value(x)
    {out.type, out.value}
  end

  ### Manipulates the update query yielding back the valid set of
  ### queries which should be executed on the database.
  defp manipulate_update_query(query, conn) do
    Logger.debug("This is an update query")

    {conn, authorization_groups} = AccessGroupSupport.calculate_access_groups(conn)

    # TODO: DRY into/from QueryAnalyzer.insert_quads

    # TODO: Check where the default_graph is used where these options are passed and verify whether this is a sensible name.
    options = %{
      default_graph: Iri.from_iri_string("<http://mu.semte.ch/application>", %{}),
      prefixes: %{
        "xsd" => Iri.from_iri_string("<http://www.w3.org/2001/XMLSchema#>"),
        "foaf" => Iri.from_iri_string("<http://xmlns.com/foaf/0.1/>")
      }
    }

    quad_in_store_with_ask =
      &(QueryAnalyzer.construct_ask_query(&1)
        |> SparqlClient.execute_parsed(request: conn, query_type: :read))["boolean"]

    analyzed_quads =
      query
      |> ALog.di("Parsed query")
      |> QueryAnalyzer.quad_changes(%{
        default_graph: Iri.from_iri_string("<http://mu.semte.ch/application>", %{}),
        authorization_groups: authorization_groups
      })
      |> Enum.reject(&match?({_, []}, &1))
      |> ALog.di("Non-empty operations")
      |> enrich_manipulations_with_access_rights(authorization_groups)
      |> maybe_verify_all_triples_written()

    case analyzed_quads do
      {:fail, reason} ->
        {:fail, reason}

      _ ->
        # From current quads, analyse what quads are already present
        triple_store_content =
          analyzed_quads
          |> Enum.flat_map(&elem(&1, 2))
          |> QueryAnalyzer.construct_asks_query()
          |> SparqlClient.execute_parsed(request: conn, query_type: :read)
          |> Map.get("results")
          |> Map.get("bindings")
          |> Enum.map(fn %{"o" => object, "s" => subject, "p" => predicate} ->
            {
              {subject["type"], subject["value"]},
              {predicate["type"], predicate["value"]},
              {object["type"], object["value"]}
            }
          end)

        # From current quads, calculate frequency of _triple_
        # Equal quads have no influence, but same triples from different graphs
        # cannot be queried with the same CONSTRUCT query
        # (because CONSTRUCT only returns triples)
        tested_content_frequencies =
          analyzed_quads
          |> Enum.flat_map(&elem(&1, 2))
          # Same things in same graph are ignored
          |> Enum.uniq()
          |> Enum.map(fn %Quad{
                           subject: subject,
                           predicate: predicate,
                           object: object,
                           graph: _graph
                         } ->
            {get_result_tuple(subject), get_result_tuple(predicate), get_result_tuple(object)}
          end)
          |> Enum.frequencies()


        # Test if a quad is inn the store
        # If the calculated frequency is one, the existence of the triple in the CONSTRUCT query
        # uniquely represents the existence of the quad in the triplestore
        # If the calculated frequency is more, the triple might exist in more graphs
        # so the CONSTRUCT query does not uniquely represent the quad in the triplestore
        # so an ASK query is executed (this shouldn't happen too often)
        quad_in_store = fn %Quad{
                             subject: subject,
                             predicate: predicate,
                             object: object,
                             graph: _graph
                           } = quad ->
          value = {get_result_tuple(subject), get_result_tuple(predicate), get_result_tuple(object)}

          if Map.get(tested_content_frequencies, value, 0) > 1 do
            quad_in_store_with_ask.(quad)
          else
            value in triple_store_content
          end
        end

        {true_inserts, true_deletions, false_inserts, false_deletions} =
          analyzed_quads
          |> Enum.map(fn {manipulation, _requested_quads, effective_quads} ->
            {manipulation, effective_quads}
          end)
          |> reduce_actual_fake(
            quad_in_store,
            {MapSet.new(), MapSet.new(), MapSet.new(), MapSet.new()}
          )

        actual_processed_manipulations = [{:delete, true_deletions}, {:insert, true_inserts}]

        executable_queries =
          actual_processed_manipulations
          |> join_quad_updates
          |> Enum.map(fn {statement, processed_quads} ->
            case statement do
              :insert ->
                QueryAnalyzer.construct_insert_query_from_quads(processed_quads, options)

              :delete ->
                QueryAnalyzer.construct_delete_query_from_quads(processed_quads, options)
            end
          end)

        delta_updater = fn ->
          Delta.publish_updates(actual_processed_manipulations, authorization_groups, conn)
        end

        # TODO: should we set the access groups on update queries too?
        # see AccessGroupSupport.put_access_groups/2 ( conn, authorization_groups )
        {conn, executable_queries, delta_updater}
    end
  end

  # Reduce :insert and :delete delta's into true and false delta's
  defp reduce_actual_fake([], _, x), do: x

  # An insert is a true delta if the quad is not yet present in the triplestore
  # If a true deletion would delete this quad, the deletion is actually a false deletion
  defp reduce_actual_fake([{:insert, quads} | xs], quad_in_store, state) do
    new_state =
      Enum.reduce(
        quads,
        state,
        fn quad, {true_inserts, true_deletions, false_inserts, false_deletions} ->
          if not quad_in_store.(quad) do
            {MapSet.put(true_inserts, quad), true_deletions, false_inserts, false_deletions}
          else
            if MapSet.member?(true_deletions, quad) do
              # Element not in store, but would be deleted
              # So both false insert and false deletion
              {true_inserts, MapSet.delete(true_deletions, quad), MapSet.put(false_inserts, quad),
               MapSet.put(false_deletions, quad)}
            else
              {true_inserts, true_deletions, MapSet.put(false_inserts, quad), false_deletions}
            end
          end
        end
      )

    reduce_actual_fake(xs, quad_in_store, new_state)
  end

  # A deletion is a true deletion if the quad is present in the triplestore
  # If a true insertion would insert this quad, the insert is actually a false insert
  defp reduce_actual_fake([{:delete, quads} | xs], quad_in_store, state) do
    new_state =
      Enum.reduce(
        quads,
        state,
        fn quad, {true_inserts, true_deletions, false_inserts, false_deletions} ->
          if quad_in_store.(quad) do
            {true_inserts, MapSet.put(true_deletions, quad), false_inserts, false_deletions}
          else
            if MapSet.member?(true_inserts, quad) do
              # Element not in store, but would be deleted
              # So both false insert and false deletion
              {MapSet.delete(true_inserts, quad), true_deletions, MapSet.put(false_inserts, quad),
               MapSet.put(false_deletions, quad)}
            else
              {true_inserts, true_deletions, false_inserts, MapSet.put(false_deletions, quad)}
            end
          end
        end
      )

    reduce_actual_fake(xs, quad_in_store, new_state)
  end


  defp enrich_manipulations_with_access_rights(manipulations, authorization_groups) do
    manipulations
    |> Enum.map(fn {kind, quads} ->
      processed_quads = enforce_write_rights(quads, authorization_groups)
      {kind, quads, processed_quads}
    end)
  end

  # If requested by configuration, this code will verify all triples
  # are going to be written to the triplestore.
  defp maybe_verify_all_triples_written(enriched_manipulations) do
    if Application.get_env(:"mu-authorization", :error_on_unwritten_data) do
      all_manipulations_complete =
        enriched_manipulations
        |> Enum.all?(fn {_manipulation, requested_quads, effective_quads} ->
          requested_triples =
            requested_quads
            |> Enum.map(&{&1.subject, &1.predicate, &1.object})
            |> MapSet.new()

          effective_triples =
            effective_quads
            |> Enum.map(&{&1.subject, &1.predicate, &1.object})
            |> MapSet.new()

          all_triples_written? = MapSet.equal?(requested_triples, effective_triples)

          unless all_triples_written? do
            Logging.EnvLog.inspect(
              MapSet.difference(requested_triples, effective_triples),
              :error,
              label: "These triples would not be written to the triplestore"
            )
          end

          all_triples_written?
        end)

      if all_manipulations_complete do
        enriched_manipulations
      else
        {:fail, "Not all triples would be written to the triplestore."}
      end
    else
      enriched_manipulations
    end
  end

  @spec join_quad_updates(QueryAnalyzer.quad_changes()) ::
          QueryAnalyzer.quad_changes()
  defp join_quad_updates(elts) do
    elts
    |> Enum.map(fn {op, quads} -> {op, MapSet.new(quads)} end)
    |> join_quad_map_updates([])
    |> Enum.map(fn {op, quads} -> {op, MapSet.to_list(quads)} end)
    |> Enum.reject(&match?({_, []}, &1))
  end

  @type map_quad :: {QueryAnalyzer.quad_change_key(), MapSet.t(Quad.t())}

  @spec join_quad_map_updates([map_quad], [map_quad]) :: [map_quad]
  defp join_quad_map_updates([], res), do: res
  defp join_quad_map_updates([elt | rest], []), do: join_quad_map_updates(rest, [elt])

  defp join_quad_map_updates([{type, quads} | rest], [{type, other_quads}]),
    do: join_quad_map_updates(rest, [{type, MapSet.union(quads, other_quads)}])

  defp join_quad_map_updates([quads | rest], [other_quads]) do
    new_other_quads = fold_quad_map_updates(other_quads, quads)
    join_quad_map_updates(rest, [new_other_quads, quads])
  end

  defp join_quad_map_updates([quad_updates | rest], [left, right]) do
    new_left = fold_quad_map_updates(left, quad_updates)
    new_right = fold_quad_map_updates(right, quad_updates)

    join_quad_map_updates(rest, [new_left, new_right])
  end

  @spec fold_quad_map_updates(map_quad, map_quad) :: map_quad
  defp fold_quad_map_updates({key, left_quads}, {key, right_quads}),
    # :inserts, :inserts or :deletes, :deletes
    do: {key, MapSet.union(left_quads, right_quads)}

  defp fold_quad_map_updates({left_type, left_quads}, {_right_type, right_quads}),
    # :inserts, :deletes or :deletes, :inserts
    do: {left_type, MapSet.difference(left_quads, right_quads)}

  @spec enforce_write_rights([Quad.t()], Acl.UserGroups.Config.t()) :: [Quad.t()]
  defp enforce_write_rights(quads, authorization_groups) do
    Logger.info("Enforcing write rights")
    user_groups_for_update = Acl.UserGroups.for_use(:write)

    processed_quads =
      quads
      |> Acl.process_quads_for_update(user_groups_for_update, authorization_groups)
      |> elem(1)
      |> ALog.di("processed quads")

    processed_quads
  end
end
