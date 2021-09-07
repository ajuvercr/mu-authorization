alias Generator.State, as: State
alias Generator.Result, as: Result
alias InterpreterTerms.Regex, as: RegexTerm
alias InterpreterTerms.Nothing, as: Nothing
alias InterpreterTerms.RegexEmitter, as: RegexEmitter

defmodule InterpreterTerms.RegexMatch do
  defstruct [:match, {:whitespace, ""}, {:external, %{}}]

  defimpl String.Chars do
    def to_string(%InterpreterTerms.RegexMatch{match: match}) do
      String.Chars.to_string({:match, match})
    end
  end
end

defmodule InterpreterTerms.Regex.Impl do
  defstruct [:regex]

  defimpl EbnfParser.ParseProtocol do
    def parse(%InterpreterTerms.Regex.Impl{regex: regex}, _parsers, chars) do
      Regex.run(regex, to_string(chars)) |> generate_result(chars, regex)
    end

    defp generate_result(nil, chars, regex) do
      [%Generator.Error{errors: ["Did not match regex " <> regex.source], leftover: chars}]
    end

    defp generate_result([string | _matches], chars, _regex) do
      [
        %Result{
          leftover: Enum.drop(chars, String.length(string)),
          matched_string: string,
          match_construct: [%InterpreterTerms.RegexMatch{match: string}]
        }
      ]
    end
  end
end

defmodule RegexEmitter do
  defstruct [:state, :known_matches]

  defimpl EbnfParser.Generator do
    def emit(%RegexEmitter{known_matches: []}) do
      {:fail}
    end

    def emit(%RegexEmitter{state: state, known_matches: [string]}) do
      {:ok, %Nothing{}, RegexEmitter.generate_result(state, string)}
    end

    def emit(%RegexEmitter{state: state, known_matches: [match | rest]} = emitter) do
      {:ok, %{emitter | known_matches: rest}, RegexEmitter.generate_result(state, match)}
    end
  end

  def generate_result(state, string) do
    %State{chars: chars} = state

    %Result{
      leftover: Enum.drop(chars, String.length(string)),
      matched_string: string,
      match_construct: [%InterpreterTerms.RegexMatch{match: string}]
    }
  end
end

defmodule InterpreterTerms.Regex do
  defstruct regex: "", state: %State{}, known_matches: []

  defimpl EbnfParser.ParserProtocol do
    def make_parser(%InterpreterTerms.Regex{regex: regex}) do
      %InterpreterTerms.Regex.Impl{
        regex: regex
      }
    end
  end

  defimpl EbnfParser.GeneratorProtocol do
    def make_generator(%RegexTerm{regex: regex, state: state} = _regex_term) do
      # Get the characters from our state
      char_string =
        state
        |> Generator.State.chars_as_string()

      # TODO be smart and use Regex.run instead
      matching_strings =
        regex
        |> Regex.scan(char_string, capture: :first)
        |> Enum.map(&Enum.at(&1, 0))

      %RegexEmitter{state: state, known_matches: matching_strings}
    end
  end
end
