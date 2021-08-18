defmodule ALog do
  require Logger

  @moduledoc """
  Simpler way to do logging through an inspect-like syntax.
  """

  @doc """
  Works like inspect, but operates on the debug logger, allowing it to
  be pruned.
  """
  def debug_inspect(name, item) do
    Logger.debug("#{name}: #{inspect(item)}")
  end

  @doc """
  Works like inspect, but is sufficiently smart to never inspect the
  supplied item when debugging.  It just passes the item through.
  Note that the form may be evaluated twice.
  """
  defmacro di(item, name, inspect_result \\ true) do
    if inspect_result do
      quote do
        result = unquote(item)
        Logger.debug(fn -> unquote(name) <> ": " <> inspect(result) end)
        result
      end
    else
      quote do
        result = unquote(item)
        Logger.debug(fn -> unquote(name) <> ": " <> result end)
        result
      end
    end
  end

  @doc """
  Equivalent to ALog.di, but for warnings.
  """
  defmacro wi(item, name, inspect_result \\ true) do
    if inspect_result do
      quote do
        result = unquote(item)
        Logger.warn(fn -> unquote(name) <> ": " <> inspect(result) end)
        result
      end
    else
      quote do
        result = unquote(item)
        Logger.warn(fn -> unquote(name) <> ": " <> result end)
        result
      end
    end
  end

  @doc """
  Works like inspect, but is sufficiently smart to never inspect the
  supplied item when going through info.  It just passes the item through.
  Note that the form may be evaluated twice.
  """
  defmacro ii(item, name, inspect_result \\ true) do
    if inspect_result do
      quote do
        result = unquote(item)
        Logger.info(fn -> unquote(name) <> ": " <> inspect(result) end)
        result
      end
    else
      quote do
        result = unquote(item)
        Logger.info(fn -> unquote(name) <> ": " <> result end)
        result
      end
    end
  end
end
