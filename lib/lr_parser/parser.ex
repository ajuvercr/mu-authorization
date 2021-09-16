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

  def tokenize(_regexes, "") do
    []
  end

  def tokenize(regexes, test_str) do
    case Enum.map(regexes, &try_regex(test_str, &1, regexes))
         |> Enum.filter(& &1) do
      [] ->
        # TODO better tokenize errors
        # IO.inspect(test_str, label: "unknown token thing")
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

  def into_expected_structure(%NonTerminal{name: :ST, construct: [x | _]}) do
    into_expected_structure(x) |> List.first()
  end

  def into_expected_structure(%NonTerminal{name: name, construct: construct}) do
    if name |> to_string |> String.starts_with?("__") do
      Enum.flat_map(construct, &into_expected_structure/1)
    else
      [
        %InterpreterTerms.SymbolMatch{
          symbol: name,
          string: "",
          submatches: Enum.flat_map(construct, &into_expected_structure/1)
        }
      ]
    end
  end

  def into_expected_structure(%LR.Grammar.Terminal.Instance{id: _, str: str}) do
    [
      %InterpreterTerms.WordMatch{
        word: str,
        whitespace: "",
        external: %{}
      }
    ]
  end

  # Used when errors occur, don't bother
  def into_expected_structure(x) do
    # Iew
    [x]
  end

  def test_ebnf() do
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

  def calculate_things(start_symbol, syntax) do
    %{terminal: terminal_forms, non_terminal: non_terminals} = syntax

    terminals =
      terminal_forms
      |> Enum.map(fn x -> EbnfParser.Sparql.split_single_form(x, true) end)
      |> Enum.map(fn {name, {_, rest}} -> {name, rest} end)

    terminal_rules = terminals |> Enum.map(fn {id, _} -> {id, [{:terminal, id}]} end)

    # all leaves of the ebnf
    flat_ebnf =
      non_terminals
      |> Enum.map(fn x -> EbnfParser.Sparql.split_single_form(x, false) end)
      |> Enum.flat_map(fn {_, {_, rest}} -> rest end)
      |> Enum.flat_map(&flatten_ebnf/1)
      |> MapSet.new()

    # In the ebnf, filter out all keywords, like "SELECT", and map them to non_terminals matched with regexes ~r/SELECT/i
    keywords =
      flat_ebnf
      |> Enum.filter(&is_word?/1)
      |> Enum.map(&elem(&1, 1))
      |> Enum.map(&word_to_token_and_regex/1)
      |> Map.new()

    # Filter out all symbols
    used_symbols =
      flat_ebnf
      |> Enum.filter(&is_symbol?/1)
      |> Enum.map(&elem(&1, 1))
      |> MapSet.new()

    # Calculate
    # Flatten terminals to simple regexes + compile them
    regexes =
      Terminal.to_terminals(terminals)
      # Reject terminals that are not used (terminals are built from regexes)
      |> Enum.filter(&MapSet.member?(used_symbols, elem(&1, 0)))
      |> Map.new()

    # Add keywords to the rules
    regexes = regexes |> Map.merge(keywords)

    # Hmmm I don't remember
    # Create :terminal symbol things that are later mapped to %Terminal{...}
    word_to_terminal = fn {_, str} = x ->
      if is_word?(x) do
        {:terminal, String.to_atom("ID_" <> str)}
      else
        x
      end
    end

    # Map ebnf things to simple rules that are used by LR.Grammar
    # [
    #   {:id, [<things>]},
    # ]
    %{some: some, simples: simples} =
      non_terminals
      |> Enum.map(fn x -> EbnfParser.Sparql.split_single_form(x, false) end)
      |> Enum.map(fn {k, {_, v}} -> {k, v} end)
      # Don't forget about the terminals
      |> Enum.concat(terminal_rules)
      # Well yeah move to {:terminal, ...} for terminals
      |> Enum.map(fn {k, v} -> {k, map_in_ebnf(v, word_to_terminal)} end)
      # EXECUTE
      |> Enum.reduce(%{simples: %{}, some: %{}}, &into_symbol_rules/2)

    # Make start things actually star things things
    rules =
      Enum.map(some, fn {[x], id} -> {[[x, id], []], id} end)
      |> Enum.concat(simples)
      # Revert map, reeeeeeeeeeeeeeeeeeeeee
      |> Enum.flat_map(fn {k, v} -> Enum.map(k, &{v, &1}) end)
      # group rules that create the same id
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    # When building actual rules pseudo groups are created
    # Some pseudo classes only have one build option, so just insert that build option!
    {singles, non_singles} =
      Enum.split_with(rules, fn {k, v} ->
        length(v) == 1 and k |> to_string |> String.starts_with?("__")
      end)

    # Singles only have one rule, flatten it
    singles = singles |> Enum.map(fn {k, [v]} -> {k, v} end) |> Map.new()

    # Map things to either itself, or single's buildrule
    remove_singles = fn x -> Map.get(singles, x, [x]) end

    non_singles =
      non_singles
      |> Enum.map(fn {k, v} ->
        vv = Enum.map(v, &Enum.flat_map(&1, remove_singles))
        {k, vv}
      end)

    # LR.Grammar expects all seperate rules
    rules = Enum.flat_map(non_singles, fn {x, v} -> Enum.map(v, &{x, &1}) end)

    # Create pseudo rule indicating start of parse
    start = {:ST, [start_symbol, LR.Grammar.Terminal.dollar()]}

    # Create state_map and start
    {start, state_map} = LR.Grammar.rules_to_state_map(start, rules)

    {regexes, start, state_map}
  end

  def generate_parser(start_symbol, syntax, module, file) do
    {regexes, start, state_map} = calculate_things(start_symbol, syntax)

    parse_f = """
      defmodule #{module} do
        def parse(str) do
          regexes = #{inspect(regexes, limit: :infinity)}
          start = #{inspect(start, limit: :infinity)}
          state_map = #{inspect(state_map, limit: :infinity)}

          tokens =
            (tokenize(regexes, str) |> Enum.map(&TerminalInstance.new/1)) ++
              [LR.Grammar.Terminal.dollar()]

          LR.Grammar.parse([start], state_map, [], tokens)
          |> into_expected_structure()
        end
      end
    """

    {:ok, file} = File.open(file, [:write, :utf8])

    IO.write(file, parse_f)
    File.close(file)

    regexes
  end

  defmacro __using__(opts) do
    syntax = opts[:syntax]
    start_symbol = opts[:start_symbol]
    {syntax, _} = Code.eval_quoted(syntax)
    {regexes, start, state_map} = LR.Parser.calculate_things(start_symbol, syntax)

    quote do
      def parse(str) do
        # {inspect(regexes, limit: :infinity)}
        # {inspect(start, limit: :infinity)}
        # {inspect(state_map, limit: :infinity)}
        regexes = unquote(regexes |> Macro.escape())
        start = unquote(start |> Macro.escape())
        state_map = unquote(state_map |> Macro.escape())

        tokens =
          (LR.Parser.tokenize(regexes, str) |> Enum.map(&TerminalInstance.new/1)) ++
            [LR.Grammar.Terminal.dollar()]

        LR.Grammar.parse([start], state_map, [], tokens)
        |> LR.Parser.into_expected_structure()
      end

      # @after_compile __MODULE__

      # def __after_compile__(env, _bytecode) do
      #   IO.inspect(unquote(start_symbol))
      #   IO.inspect(unquote(syntax))

      #   Module.put_attribute(__MODULE__, :Regexes, regexes)
      #   Module.put_attribute(__MODULE__, :Regexes, regexes)
      #   Module.put_attribute(__MODULE__, :Regexes, regexes)

      #   # LR.Parser.calculate_things(unquote(start_symbol), unquote(syntax))
      #   # |> IO.inspect()
      # end
    end
  end
end
