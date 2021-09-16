alias LR.Grammar.Terminal
alias LR.Grammar.Terminal.Instance, as: TerminalInstance
alias LR.Grammar.NonTerminal
alias LR.Grammar.Item
alias LR.Grammar.ItemSet
alias LR.Grammar.Transition
alias LR.Grammar.Action

defmodule LR.Parser do
  defp flatten_ebnf({ty, things})
       when ty in [:one_of, :paren_group, :maybe, :maybe_many, :one_or_more] do
    Enum.flat_map(things, &flatten_ebnf/1)
  end

  defp flatten_ebnf(x) do
    [x]
  end

  defp map_in_ebnf({ty, things}, f)
       when ty in [:one_of, :paren_group, :maybe, :maybe_many, :one_or_more] do
    {ty, things |> Enum.map(&map_in_ebnf(&1, f))}
  end

  defp map_in_ebnf(x, f)
       when is_list(x) do
    x |> Enum.map(&map_in_ebnf(&1, f))
  end

  defp map_in_ebnf(x, f) do
    f.(x)
  end

  defp is_word?({ty, _}) when ty in [:single_quoted_string, :double_quoted_string] do
    true
  end

  defp is_word?(_) do
    false
  end

  defp is_symbol?({:symbol, _}) do
    true
  end

  defp is_symbol?(_) do
    false
  end

  defp word_to_token_and_regex(str) do
    {String.to_atom("ID_" <> str), ~r/(\A#{Regex.escape(str)})\s*/i}
  end

  defp try_regex("", _, _) do
    nil
  end

  defp try_regex(test_str, {id, regex}, regexes) do
    case Regex.run(regex, test_str) do
      nil ->
        nil

      [matched, terminal | _] ->
        {_, rest} = String.split_at(test_str, String.length(matched))
        [{id, terminal} | tokenize(regexes, rest)]
    end
  end

  defp max_result([{_, t} | _]) do
    String.length(t)
  end

  defp tokenize(_regexes, "") do
    []
  end

  defp tokenize(regexes, test_str) do
    case Enum.map(regexes, &try_regex(test_str, &1, regexes))
         |> Enum.filter(& &1) do
      [] ->
        # TODO better tokenize errors
        IO.inspect(test_str, label: "unknown token thing")
        []

      rest ->
        rest |> Enum.max_by(&max_result/1)
    end
  end

  defp gen_id do
    id = System.unique_integer([:positive])
    ("__" <> Integer.to_string(id)) |> String.to_atom()
  end

  defp insert_if_not_present(items, %{simples: new_dict} = things) do
    {id, new_dict} =
      if Map.has_key?(new_dict, items) do
        {Map.get(new_dict, items), new_dict}
      else
        id = gen_id()
        {id, new_dict |> Map.put(items, id)}
      end

    {id, %{things | simples: new_dict}}
  end

  defp insert_if_not_present_some(items, %{some: new_dict} = things) do
    {id, new_dict} =
      if Map.has_key?(new_dict, items) do
        {Map.get(new_dict, items), new_dict}
      else
        id = gen_id()
        {id, new_dict |> Map.put(items, id)}
      end

    {id, %{things | some: new_dict}}
  end

  defp into_rules({:symbol, atom}, dict) do
    {atom, dict}
  end

  defp into_rules({:terminal, atom}, dict) do
    {%Terminal{id: atom}, dict}
  end

  defp into_rules({:one_of, items}, dict) do
    {items, new_dict} = Enum.map_reduce(items, dict, &into_rules/2)
    insert_if_not_present(items |> Enum.map(&[&1]), new_dict)
  end

  # # # TODO special case if only one item
  # defp into_rules({:paren_group, [x]}, dict) do
  #   into_rules(x, dict)
  # end

  defp into_rules({:paren_group, items}, dict) do
    {items, new_dict} = Enum.map_reduce(items, dict, &into_rules/2)
    insert_if_not_present([items], new_dict)
  end

  defp into_rules({:maybe, items}, dict) do
    {items, new_dict} = Enum.map_reduce(items, dict, &into_rules/2)
    insert_if_not_present([items, []], new_dict)
  end

  defp into_rules({:maybe_many, items}, dict) do
    {items, new_dict} = Enum.map_reduce(items, dict, &into_rules/2)
    insert_if_not_present_some(items, new_dict)
  end

  defp into_rules({:one_or_more, items}, dict) do
    {some, new_dict} = into_rules({:maybe_many, items}, dict)
    {items, new_dict} = Enum.map_reduce(items, new_dict, &into_rules/2)
    insert_if_not_present([items ++ [some]], new_dict)
  end

  defp into_symbol_rules({atom, items}, dict) do
    {items, %{simples: simples, some: some}} = Enum.map_reduce(items, dict, &into_rules/2)
    simples = simples |> Map.put([items], atom)

    %{simples: simples, some: some}
  end

  defp test_ebnf() do
    %{
      non_terminal: [
        "Expression ::= '(' Expression ')' | Times",
        "Times ::= (POS '*' Expression) | Addition",
        "Addition ::= (POS '+' Expression) | POS"
      ],
      terminal: [
        "POS ::= '-'? [0-9]+"
      ]
    }
  end

  def test(test_str) do
    # EbnfParser.Forms.sparql()
    %{terminal: terminal_forms, non_terminal: non_terminals} = EbnfParser.Forms.sparql() # test_ebnf()

    terminals =
      terminal_forms
      |> Enum.map(fn x -> EbnfParser.Sparql.split_single_form(x, true) end)
      |> Enum.map(fn {name, {_, rest}} -> {name, rest} end)
      |> IO.inspect(label: "terminals")

    terminal_rules = terminals |> Enum.map(fn {id, _} -> {id, [{:terminal, id}]} end)

    flat_ebnf =
      non_terminals
      |> Enum.map(fn x -> EbnfParser.Sparql.split_single_form(x, false) end)
      |> Enum.flat_map(fn {_, {_, rest}} -> rest end)
      |> Enum.flat_map(&flatten_ebnf/1)

    keywords =
      flat_ebnf
      |> Enum.filter(&is_word?/1)
      |> Enum.map(&elem(&1, 1))
      |> MapSet.new()
      |> Enum.map(&word_to_token_and_regex/1)
      |> Map.new()

    used_symbols =
      flat_ebnf
      |> Enum.filter(&is_symbol?/1)
      |> Enum.map(&elem(&1, 1))
      |> MapSet.new()

    regexes =
      Terminal.to_terminals(terminals)
      |> Enum.filter(&MapSet.member?(used_symbols, elem(&1, 0)))
      |> Map.new()

    regexes = regexes |> Map.merge(keywords)

    word_to_terminal = fn {_, str} = x ->
      if is_word?(x) do
        {:terminal, String.to_atom("ID_" <> str)}
      else
        x
      end
    end

    %{some: some, simples: simples} =
      non_terminals
      |> Enum.map(fn x -> EbnfParser.Sparql.split_single_form(x, false) end)
      |> Enum.map(fn {k, {_, v}} -> {k, v} end)
      |> Enum.concat(terminal_rules)
      |> Enum.map(fn {k, v} -> {k, map_in_ebnf(v, word_to_terminal)} end)
      |> Enum.reduce(%{simples: %{}, some: %{}}, &into_symbol_rules/2)

    # Make some things actually some things
    rules =
      Enum.map(some, fn {[x], id} -> {[[x, id], []], id} end)
      |> Enum.concat(simples)
      |> Enum.flat_map(fn {k, v} -> Enum.map(k, &{v, &1}) end)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    {singles, non_singles} =
      Enum.split_with(rules, fn {k, v} ->
        length(v) == 1 and k |> to_string |> String.starts_with?("__")
      end)

    singles = singles |> Enum.map(fn {k, [v]} -> {k, v} end) |> Map.new() |> IO.inspect(label: "Singles")
    remove_singles = fn x -> Map.get(singles, x, [x]) end

    non_singles =
      non_singles
      |> Enum.map(fn {k, v} ->
        vv = Enum.map(v, &Enum.flat_map(&1, remove_singles))
        {k, vv}
      end)
      |> IO.inspect(label: "Non singles")

    rules = Enum.flat_map(non_singles, fn {x, v} -> Enum.map(v, &{x, &1}) end)
    |> IO.inspect(label: "Rules")

    # simples
    start = {:ST, [:Sparql, LR.Grammar.Terminal.dollar()]}

    {start, state_map} = LR.Grammar.rules_to_state_map(start, rules)

    # # start
    # IO.puts(ItemSet.to_string(start))
    # rules
    #   |> Enum.flat_map(&elem(&1, 1))
    #   |> Enum.filter(&is_list/1)

    tokens =
      (tokenize(regexes, test_str) |> Enum.map(&TerminalInstance.new/1)) ++
        [LR.Grammar.Terminal.dollar()]

    LR.Grammar.parse([start], state_map, [], tokens)

    # nil
    # map_size(state_map)

    # |> Map.new()
    # nil
    # non_terminals
  end
end
