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
end
