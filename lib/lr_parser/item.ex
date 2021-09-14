alias LR.Grammar.Item

defmodule Item do
  defstruct [:rule, :index]

  defimpl String.Chars, for: Item do
    def to_string(%Item{rule: {name, parts}, index: index}) do
      interior =
        parts
        |> Enum.map(&part_to_string/1)
        |> List.insert_at(index, '.')
        |> Enum.join(" ")

      "'#{inspect(name)} -> #{interior}'"
    end

    defp part_to_string(x) when is_atom(x) do
      ":" <> Kernel.to_string(x)
    end

    defp part_to_string(x) do
      Kernel.to_string(x)
    end
  end

  @spec new(any) :: %LR.Grammar.Item{index: 0, rule: any}
  def new(rule) do
    %Item{rule: rule, index: 0}
  end

  def name(%Item{rule: {n, _}}) do
    n
  end

  def next(%Item{rule: {_, parts}, index: index}) do
    Enum.at(parts, index, nil)
  end

  def eat(%Item{rule: {_, parts}, index: index} = item) do
    if length(parts) == index do
      nil
    else
      %{item | index: index + 1}
    end
  end

  def parts(%Item{rule: {_, parts}}) do
    parts
  end

  def end?(%Item{rule: {_, parts}, index: index}) do
    length(parts) == index
  end
end
