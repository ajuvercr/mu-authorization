alias LR.Grammar.NonTerminal

defmodule NonTerminal do
  defstruct [:name, :construct]

  def new(name, construct) do
    %NonTerminal{name: name, construct: construct}
  end

  defimpl LR.Rule, for: Terminal do
    def nullable(%NonTerminal{} = rule) do
      first(rule) |> Enum.any?(&LR.Rule.nullable/1)
    end

    def first(%NonTerminal{construct: construct}) do
      Enum.reduce_while(construct, MapSet.new, fn el, acc ->
        if LR.Rule.nullable(el) do
          {:cont, MapSet.put(acc, el)}
        else
          {:halt, MapSet.put(acc, el)}
        end
      end)
    end

    def follow(%NonTerminal{construct: construct}) do
      Enum.reduce_while(construct, MapSet.new, fn el, acc ->
        if LR.Rule.nullable(el) do
          {:cont, MapSet.union(acc, LR.Rule.first(el))}
        else
          {:halt, MapSet.union(acc, LR.Rule.first(el))}
        end
      end)
    end
  end
end
