alias LR.Grammar.Terminal

defmodule Terminal do
  defstruct [:str]

  defimpl String.Chars, for: Terminal do
    def to_string(%Terminal{str: str}) do
      str
    end
  end

  defimpl LR.Rule, for: Terminal do
    def nullable(%Terminal{} = rule) do
      Terminal.match(rule, "")
      # first(rule) |> Enum.any?(&LR.Rule.nullable/1)
    end

    def first(%Terminal{} = rule) do
      MapSet.new()
      |> MapSet.put(rule)

      # rule
      # Enum.reduce_while()
      # Enum.take_while()
    end

    def follow(%Terminal{}) do
      MapSet.new()
    end
  end

  def new(str) do
    %Terminal{str: str}
  end

  def nullable?(%Terminal{str: str}) do
    str == ""
  end

  def match(%Terminal{str: str}, test_str) do
    str == test_str
  end

  def dollar do
    %Terminal{str: "$"}
  end

  def lparen do
    %Terminal{str: "("}
  end

  def rparen do
    %Terminal{str: ")"}
  end

  def comma do
    %Terminal{str: ","}
  end

  def plus do
    %Terminal{str: "+"}
  end

  def x do
    %Terminal{str: "x"}
  end

  def empty do
    %Terminal{str: ""}
  end

  def space do
    %Terminal{str: "_"}
  end

  defp flatten_part({:symbol, sym}, forms) do
    {:paren_group, forms[sym]}
  end

  defp flatten_part({:paren_group, [x]}, forms) do
    flatten_part(x, forms)
  end

  defp flatten_part({ty, things}, forms)
       when ty in [:one_of, :paren_group, :maybe, :maybe_many] do
    {ty, Enum.map(things, &flatten_part(&1, forms))}
  end

  defp flatten_part(x, _forms) do
    x
  end

  defp has_symbol?({:symbol, _}) do
    true
  end

  defp has_symbol?({ty, things})
       when ty in [:one_of, :paren_group, :maybe, :maybe_many, :one_or_more] do
    Enum.any?(things, &has_symbol?/1)
  end

  defp has_symbol?(_) do
    false
  end

  defp flatten(forms) do
    new_forms =
      Enum.map(forms, fn {key, things} ->
        new_things = Enum.map(things, &flatten_part(&1, forms))
        {key, new_things}
      end)

    if new_forms |> Enum.flat_map(&elem(&1, 1)) |> Enum.any?(&has_symbol?/1) do
      flatten(new_forms)
    else
      new_forms
    end
  end

  defp to_regex({:single_quoted_string, v}) do
    Regex.escape(v)
  end

  defp to_regex({:double_quoted_string, v}) do
    Regex.escape(v)
  end

  defp to_regex({:character, v}) do
    Regex.escape(v)
  end

  defp to_regex({:hex_character, 0}) do
    IO.puts("HEREHER HERE")
    "\x00"
  end

  defp to_regex({:hex_character, v}) do
    x = <<v::utf8>>
    Regex.escape(x)
  end

  defp to_regex({:range, [foo, bar]}) do
    "#{to_regex(foo)}-#{to_regex(bar)}"
  end

  defp to_regex({:minus, [foo, bar]}) do
    "(#{to_regex(foo)}(?!#{to_regex(bar)}))"
  end

  defp to_regex({:bracket_selector, things}) do
    nt = things |> Enum.map(&to_regex/1) |> Enum.join()
    "[#{nt}]"
  end

  defp to_regex({:not_bracket_selector, things}) do
    nt = things |> Enum.map(&to_regex/1) |> Enum.join()
    "[^#{nt}]"
  end

  defp to_regex({:paren_group, things}) do
    nt = things |> Enum.map(&to_regex/1) |> Enum.join()
    "(#{nt})"
  end

  defp to_regex({:one_of, things}) do
    nt = things |> Enum.map(&to_regex/1) |> Enum.join("|")
    "(#{nt})"
  end

  defp to_regex({:one_or_more, things}) do
    nt = things |> Enum.map(&to_regex/1) |> Enum.join("")
    "(#{nt})+"
  end

  defp to_regex({:maybe, things}) do
    nt = things |> Enum.map(&to_regex/1) |> Enum.join("")
    "(#{nt})?"
  end

  defp to_regex({:maybe_many, things}) do
    nt = things |> Enum.map(&to_regex/1) |> Enum.join("")
    "(#{nt})*"
  end

  defp to_regex(things) when is_list(things) do
    things |> Enum.map(&to_regex/1) |> Enum.join("")
  end

  # defp try_compile(x) do
  #   tx = x |> String.graphemes() |> Enum.filter(&String.valid?/1) |> Enum.map(&to_string/1)

  #   case Regex.compile("(#{x})") do
  #     {:ok, x} -> x
  #     {:error, reason} -> {:error, reason, tx}
  #   end
  # end

  defp do_compile(x) do
    # Regex.compile!("\A(#{x})\s*")
    ~r/\A(#{x})\s*/
  end

  def to_terminals(terminal_forms) do
    terminal_forms
    |> flatten()
    |> Enum.map(fn {name, rest} -> {name, rest |> to_regex() |> do_compile()} end)

    # |> IO.inspect()
  end
end
