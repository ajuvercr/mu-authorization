alias LR.Grammar.Terminal.Instance, as: TerminalInstance

defmodule LR.Parser.Server do
  use GenServer

  @impl true
  def init(state) do
    {:ok, state}
  end

  def start_link({start_symbol, syntax}) do
    state = LR.Parser.calculate_things(start_symbol, syntax)

    GenServer.start_link(__MODULE__, state, name: start_symbol)
  end

  def parse_as(symbol, str) do
    GenServer.call(symbol, {:parse, str})
  end

  @impl true
  def handle_call({:parse, str}, _from, {regexes, start, state_map} = state) do
    id = Profiler.start("tokenize")

    tokens =
      (LR.Parser.tokenize(regexes, str) |> Enum.map(&TerminalInstance.new/1)) ++
        [LR.Grammar.Terminal.dollar()]

    Profiler.stop(id)

    id = Profiler.start("parse")

    parsed =
      LR.Grammar.parse([start], state_map, [], tokens)
      |> LR.Parser.into_expected_structure()

    Profiler.stop(id)

    {:reply, parsed, state}
  end
end
