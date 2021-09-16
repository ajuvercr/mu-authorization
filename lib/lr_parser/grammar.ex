alias LR.Grammar.Terminal
alias LR.Grammar.Terminal.Instance, as: TerminalInstance
alias LR.Grammar.NonTerminal
alias LR.Grammar.Item
alias LR.Grammar.ItemSet
alias LR.Grammar.Transition
alias LR.Grammar.Action

defmodule LR.Grammar do
  import Terminal

  def rules do
    [
      {:ST, [:S, dollar()]},
      {:S, [:WS2, lparen(), :WL, rparen()]},
      {:WL, [:WS, :L]},
      {:S, [x()]},
      {:L, [:S]},
      {:L, [:L, comma(), :S]},
      {:L, []},
      {:S, []},
      {:WS, [space()]},
      {:WS2, [space()]},
      {:WS2, []}

      # {:PLUS, [plus()]},
      # {:COMMA, [comma()]}
    ]
  end

  def rules2 do
    [
      {:ST, [:S, dollar()]},
      {:S, [:T, plus(), :S]},
      {:S, [:T]},
      {:T, [x()]}
    ]
  end

  def rules_dict(rules) do
    rules
    |> Enum.group_by(&elem(&1, 0))
  end

  def test_things do
    [lparen(), x(), comma(), x(), rparen(), dollar()]
  end

  def get_prod(nonterminal, rules_dict) do
    Map.get(rules_dict, nonterminal, [])
  end

  def get_key(%NonTerminal{name: name}) do
    name
  end

  def get_key(%TerminalInstance{id: id}) do
    %Terminal{id: id}
  end

  def get_key(x) do
    x
  end

  def closure(set, rules_dict) do
    ns =
      Enum.reduce(set, set, fn x, set ->
        ItemSet.add_items(set, get_prod(Item.next(x), rules_dict))
      end)

    if ItemSet.size(ns) > ItemSet.size(set) do
      closure(ns, rules_dict)
    else
      set
    end
  end

  def goto(set, item, rules_dict) do
    set
    |> Enum.filter(&(Item.next(&1) == item))
    |> Enum.map(&Item.eat/1)
    |> Enum.filter(& &1)
    |> ItemSet.new()
    |> closure(rules_dict)
  end

  def reducers(t, follows) do
    Enum.reduce(t, %{}, fn i, r ->
      # set_state = %{reducers: , transitions: %{}}
      set_state =
        Enum.filter(i, &Item.end?/1)
        |> Enum.flat_map(fn item ->
          follows
          |> Map.get(Item.name(item), [nil])
          |> Enum.map(&{&1, item})
        end)
        |> Map.new()
        |> Action.new()

      Map.put(r, i, set_state)
    end)
  end

  def create(t, e, rules_dict) do
    {nt, ne} =
      Enum.reduce(t, {t, e}, fn i, acc ->
        Enum.reduce(i, acc, fn item, {t, e} ->
          eaten = Item.next(item)
          j = goto(i, eaten, rules_dict)

          if Enum.empty?(j) do
            {t, e}
          else
            nt = MapSet.put(t, j)
            ne = MapSet.put(e, Transition.new(eaten, i, j))
            {nt, ne}
          end
        end)
      end)

    if nt == t and ne == e do
      {t, e}
    else
      create(nt, ne, rules_dict)
    end
  end

  def nullable(nullable_set, rules) do
    new_set =
      rules
      |> Enum.reduce(nullable_set, fn {name, parts}, set ->
        if Enum.all?(parts, &MapSet.member?(set, &1)) do
          MapSet.put(set, name)
        else
          set
        end
      end)

    if MapSet.size(new_set) > MapSet.size(nullable_set) do
      new_set |> nullable(rules)
    else
      new_set
    end
  end

  defp get_to_first_not_nullables([], _nullables) do
    []
  end

  defp get_to_first_not_nullables([x | xs], nullables) do
    if MapSet.member?(nullables, x) do
      [x | get_to_first_not_nullables(xs, nullables)]
    else
      [x]
    end
  end

  def calc_firsts(firsts, nullables, rules) do
    new_firsts =
      Enum.reduce(rules, firsts, fn {name, parts}, firsts ->

        rule_firsts =
          get_to_first_not_nullables(parts, nullables)
          |> Enum.reduce(firsts[name], &MapSet.union(firsts[&1], &2))

        Map.put(firsts, name, rule_firsts)
      end)

    if new_firsts == firsts do
      new_firsts
    else
      calc_firsts(new_firsts, nullables, rules)
    end
  end

  defp calc_follow_with_rule({name, parts}, follows, firsts, nullables) do
    mine = Map.get(follows, name, MapSet.new())

    new_follows =
      parts
      |> Enum.reverse()
      |> get_to_first_not_nullables(nullables)
      |> Enum.reduce(follows, fn t, follows ->
        Map.update(follows, t, mine, &MapSet.union(mine, &1))
      end)

    calc_follow_from_parts(parts, new_follows, firsts, nullables)
  end

  def calc_follow_from_parts([], follows, _firsts, _nullables) do
    follows
  end

  def calc_follow_from_parts([x | xs], follows, firsts, nullables) do
    new_follows =
      get_to_first_not_nullables(xs, nullables)
      |> Enum.reduce(MapSet.new(), &MapSet.union(&2, Map.get(firsts, &1, MapSet.new())))

    calc_follow_from_parts(xs, follows, firsts, nullables)
    |> Map.update(x, new_follows, &MapSet.union(new_follows, &1))
  end

  def calc_follow(follows, firsts, nullables, rules) do
    new_follows =
      rules
      |> Enum.reduce(follows, fn rule, follows ->
        calc_follow_with_rule(rule, follows, firsts, nullables)
      end)

    if new_follows == follows do
      follows
    else
      calc_follow(new_follows, firsts, nullables, rules)
    end
  end

  def get_path(atom) do
    atom
  end

  def parse([], _transitions, stack, queue) do
    {stack, queue, :there}
  end

  def parse([state | s_rest], transitions, stack, queue) do
    queue_head = List.first(queue, nil)
    transition = transitions[state]

    case Map.get(transition.reducers, get_key(queue_head), nil) do
      nil ->
        IO.puts("shift")

        trans = transition.transitions
        # TODO: make x like {x, something something}
        [x | xs] = queue

        case trans[get_key(x)] do
          nil ->
            {trans, state, stack, queue, :here}

          new_state ->
            parse([new_state, state | s_rest], transitions, [x | stack], xs)
        end

      item ->
        IO.puts("reduce")
        {base, stack_rest} = Enum.split(stack, item.index)

        s_rest = Enum.drop([state | s_rest], item.index)

        {thing, _} = item.rule

        # TODO: make x like {x, something something}
        thing = NonTerminal.new(thing, base |> Enum.reverse())
        # new_stack = [thing| stack_rest]
        if List.first(base) == dollar() do
          thing
        else
          parse(s_rest, transitions, stack_rest, [thing | queue])
        end
    end
  end

  def rules_to_state_map(start, rules) do
    rules = [start | rules]
    rules_dict = rules |> rules_dict()

    nullables =
      rules
      # Get empty rules
      |> Enum.filter(&Enum.empty?(elem(&1, 1)))
      # Map to id
      |> Enum.map(&elem(&1, 0))
      |> MapSet.new()
      # Calculate other nullables
      |> nullable(rules)
      |> IO.inspect(label: "nullables")

    all_parts =
      rules |> Enum.flat_map(&elem(&1, 1)) |> Enum.concat(rules |> Enum.map(&elem(&1, 0)))

    firsts =
      all_parts
      |> Enum.map(fn x ->
        IO.inspect(x, label: "doing this")

        case x do
          %Terminal{} ->
            {x, MapSet.new() |> MapSet.put(x)}

          _ ->
            {x, MapSet.new()}
        end
      end)
      |> Map.new()
      |> calc_firsts(nullables, rules)
      |> IO.inspect(label: "Firsts")

    follows =
      calc_follow(%{}, firsts, nullables, rules)
      |> IO.inspect(label: "follows")

    # Append dollar() to state you want to parse to
    start =
      ItemSet.add_item(ItemSet.new(), start)
      |> closure(rules_dict)

    # Currently the only passed state is the start_state
    start_states = MapSet.new() |> MapSet.put(start)

    # Calculate all states `t` and transitions `e`
    {t, e} = create(start_states, MapSet.new(), rules_dict)

    # Start transition table with reduces actions
    state_map = reducers(t, follows)

    # Add transitions to transition table
    {start, Enum.reduce(e, state_map, &Action.add_transition_in/2)}
  end

  def test(test_str \\ "(x,x)") do
    rules = rules()
    {start, state_map} = rules_to_state_map({:ST, [:S, dollar()]}, rules)

    # Enum.each(t, fn t ->
    #   Enum.each(t, fn x -> IO.puts(to_string(x)) end)
    # end)

    # Parse
    tokens = (test_str <> "$") |> String.graphemes() |> Enum.map(&Terminal.new/1)
    parse([start], state_map, [], tokens)

    # :ok
  end
end
