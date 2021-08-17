defmodule Cache.Deltas do
  alias Updates.QueryAnalyzer
  alias Updates.QueryAnalyzer.P, as: QueryAnalyzerProtocol
  alias Updates.QueryAnalyzer.Types.Quad, as: Quad

  require Logger
  require ALog
  use GenServer

  @type cache_logic_key :: :precache | :construct | :ask

  defp new_cache, do: {%{}, %{}, %{}, %{}}

  ### GenServer API
  @doc """
    GenServer.init/1 callback
  """
  def init(state) do
    state = state || %{metas: [], cache: new_cache(), index: :os.system_time(:millisecond)}
    {:ok, state}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc """
    Flush the current state, actually applying the delta's to the triplestore.
  """
  def flush(options) do
    GenServer.call(__MODULE__, {:flush, options})
  end

  @spec add_deltas(QueryAnalyzer.quad_changes(), cache_logic_key()) :: :ok
  def add_deltas(quad_changes, logic, delta_meta \\ []) do
    case logic do
      :precache -> GenServer.cast(__MODULE__, {:cache_w_cache, quad_changes})
      :construct -> GenServer.cast(__MODULE__, {:cache_w_construct, quad_changes, delta_meta})
      :ask -> GenServer.cast(__MODULE__, {:cache_w_ask, quad_changes})
    end
  end

  ## Create tuple from literal {type, value}
  defp get_result_tuple(x) do
    out = QueryAnalyzerProtocol.to_sparql_result_value(x)
    {out.type, out.value}
  end

  defp quad_in_store_with_ask?(quad) do
    (QueryAnalyzer.construct_ask_query(quad)
     |> SparqlClient.execute_parsed(query_type: :read))["boolean"]
  end

  # From current quads, analyse what quads are already present
  defp quads_in_store_with_construct(quads) do
    quads
    |> QueryAnalyzer.construct_asks_query()
    |> SparqlClient.execute_parsed(query_type: :read)
    |> Map.get("results")
    |> Map.get("bindings")
    |> Enum.map(fn %{"o" => object, "s" => subject, "p" => predicate} ->
      {
        {subject["type"], subject["value"]},
        {predicate["type"], predicate["value"]},
        {object["type"], object["value"]}
      }
    end)
    |> MapSet.new()
  end

  # From current quads, calculate frequency of _triple_
  # Equal quads have no influence, but same triples from different graphs
  # cannot be queried with the same CONSTRUCT query
  # (because CONSTRUCT only returns triples)
  defp triple_counts_with_graph_differences(quads) do
    quads
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
  end

  # Test if a quad is inn the store
  # If the calculated frequency is one, the existence of the triple in the CONSTRUCT query
  # uniquely represents the existence of the quad in the triplestore
  # If the calculated frequency is more, the triple might exist in more graphs
  # so the CONSTRUCT query does not uniquely represent the quad in the triplestore
  # so an ASK query is executed (this shouldn't happen too often)
  defp quad_in_store?(
         %{triple_counts: triple_counts, triples_in_store: triples_in_store},
         %Quad{
           subject: subject,
           predicate: predicate,
           object: object,
           graph: _graph
         } = quad
       ) do
    value = {get_result_tuple(subject), get_result_tuple(predicate), get_result_tuple(object)}

    if Map.get(triple_counts, value, 0) > 1 do
      quad_in_store_with_ask?(quad)
    else
      value in triples_in_store
    end
  end

  # Reduce :insert and :delete delta's into true and false delta's
  #
  # An insert is a true delta if the quad is not yet present in the triplestore
  # If a true deletion would delete this quad, the deletion is actually a false deletion
  defp add_delta_to_state({:insert, quad}, state) do
    meta = List.first(state.metas)
    {true_inserts, true_deletions, false_inserts, false_deletions} = state.cache

    new_cache =
      if quad_in_store?(meta, quad) do
        if Map.has_key?(true_deletions, quad) do
          # Element not in store, but would be deleted
          # So both false insert and false deletion
          {original_index, true_deletions} = Map.pop!(true_deletions, quad)

          {true_inserts, Map.delete(true_deletions, quad),
           Map.put(false_inserts, quad, state.index),
           Map.put(false_deletions, quad, original_index)}
        else
          {true_inserts, true_deletions, Map.put(false_inserts, quad, state.index),
           false_deletions}
        end
      else
        {Map.put(true_inserts, quad, state.index), true_deletions, false_inserts, false_deletions}
      end

    %{state | cache: new_cache}
  end

  # A deletion is a true deletion if the quad is present in the triplestore
  # If a true insertion would insert this quad, the insert is actually a false insert
  defp add_delta_to_state({:delete, quad}, state) do
    meta = List.first(state.metas)
    {true_inserts, true_deletions, false_inserts, false_deletions} = state.cache

    new_cache =
      if quad_in_store?(meta, quad) do
        {true_inserts, Map.put(true_deletions, quad, state.index), false_inserts, false_deletions}
      else
        if Map.has_key?(true_inserts, quad) do
          # Element not in store, but would be deleted
          # So both false insert and false deletion
          {original_index, true_inserts} = Map.pop!(true_inserts, quad)

          {true_inserts, true_deletions, Map.put(false_inserts, quad, original_index),
           Map.put(false_deletions, quad, state.index)}
        else
          {true_inserts, true_deletions, false_inserts,
           Map.put(false_deletions, quad, state.index)}
        end
      end

    %{state | cache: new_cache}
  end

  defp convert_quad(%Quad{graph: graph, subject: subject, predicate: predicate, object: object}) do
    [g, s, p, o] =
      Enum.map(
        [graph, subject, predicate, object],
        &QueryAnalyzerProtocol.to_sparql_result_value/1
      )

    %{"graph" => g, "subject" => s, "predicate" => p, "object" => o}
  end

  defp delta_update(state) do
    {true_inserts, true_deletions, _false_inserts, _false_deletions} = state.cache

    # content = Enum.map(true_inserts, &({:insert, &1})) ++ Enum.map(true_deletions, &({:delete, &1}))

    inserts = Enum.group_by(true_inserts, &elem(&1, 1), &{:insert, convert_quad(elem(&1, 0))})
    deletions = Enum.group_by(true_deletions, &elem(&1, 1), &{:delete, convert_quad(elem(&1, 0))})
    total = Map.merge(inserts, deletions, fn _, one, two -> one ++ two end)

    messages =
      Enum.map(state.metas, fn meta ->
        index = meta.index

        other_meta =
          meta.delta_meta
          |> Map.new()
          |> Map.put(:index, index)

        Map.get(total, index, [])
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
        |> Map.merge(other_meta)
      end)

    %{
      "changeSets" => messages
    }
    |> Poison.encode!()
    |> Delta.Messenger.inform_clients()
  end

  @doc """
    GenServer.handle_call/3 callback
  """
  def handle_call({:flush, options}, _from, state) do
    {true_inserts, true_deletions, _false_inserts, _false_deletions} = state.cache

    inserts = Map.keys(true_inserts)

    unless Enum.empty?(inserts) do
      QueryAnalyzer.construct_insert_query_from_quads(inserts, options)
      |> SparqlClient.execute_parsed(query_type: :write)
      |> ALog.di("Results from SparqlClient after write")
    end

    deletions = Map.keys(true_deletions)

    unless Enum.empty?(deletions) do
      QueryAnalyzer.construct_delete_query_from_quads(deletions, options)
      |> SparqlClient.execute_parsed(query_type: :write)
      |> ALog.di("Results from SparqlClient after write")
    end

    delta_update(state)

    new_state = %{state | cache: new_cache(), metas: []}

    {:reply, :ok, new_state}
  end

  # delta_meta: mu_call_id_trail, authorization_groups, origin
  def handle_cast({:cache_w_construct, quads, delta_meta}, state) do
    deltas = Enum.flat_map(quads, fn {type, qs} -> Enum.map(qs, &{type, &1}) end)
    quads = Enum.map(deltas, &elem(&1, 1))

    # Calculate meta data
    triple_counts = triple_counts_with_graph_differences(quads)
    triples_in_store = quads_in_store_with_construct(quads)

    # Add metadata to state
    meta = %{
      triple_counts: triple_counts,
      triples_in_store: triples_in_store,
      delta_meta: delta_meta,
      index: state.index + 1
    }

    state_with_meta = %{state | metas: [meta | state.metas], index: state.index + 1}

    # Reduce with add_delta_to_state
    new_state = Enum.reduce(deltas, state_with_meta, &add_delta_to_state/2)

    {:noreply, new_state}
  end

  def handle_cast({:cache_w_ask, _quads}, state) do
    {:noreply, state}
  end
end