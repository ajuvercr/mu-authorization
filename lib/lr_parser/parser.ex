alias LR.Grammar.Terminal

defmodule LR.Parser do
  defp flatten_ebnf({ty, things})
       when ty in [:one_of, :paren_group, :maybe, :maybe_many] do
    Enum.flat_map(things, &flatten_ebnf/1)
  end

  defp flatten_ebnf(x) do
    [x]
  end

  defp map_in_ebnf({ty, things}, f)
       when ty in [:one_of, :paren_group, :maybe, :maybe_many] do
    {ty, things |> Enum.map(&map_in_ebnf(&1, f))}
  end

  defp map_in_ebnf(x, f)
       when is_list(x) do
    x|> Enum.map(&map_in_ebnf(&1, f))
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

  def test(test_str) do
    %{terminal: terminal_forms, non_terminal: non_terminals} = EbnfParser.Forms.sparql()

    terminals =
      terminal_forms
      |> Enum.map(fn x -> EbnfParser.Sparql.split_single_form(x, true) end)
      |> Enum.map(fn {name, {_, rest}} -> {name, rest} end)

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
        {:symbol, String.to_atom("ID_" <> str)}
      else
        x
      end
    end

    tokenize(regexes, test_str)

    non_terminals
    |> Enum.map(fn x -> EbnfParser.Sparql.split_single_form(x, false) end)
    |> Enum.map(fn {k, {_, v}} -> {k, v} end)
    |> Enum.map(fn {k, v} -> {k, map_in_ebnf(v, word_to_terminal)} end)
    |> Map.new()

    # nil
    # non_terminals
  end
end
