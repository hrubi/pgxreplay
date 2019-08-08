defmodule Pgxreplay do
  @moduledoc """
  Pgxreplay replays statements in a postgres log.

  The postgresql.conf has to be configured with:

    log_min_messages = error
    log_min_error_statement = log  # (or more)
    log_connections = on
    log_disconnections = on
    log_line_prefix = '%m|%u|%d|%c|'
    log_statement = 'all'`
    # lc_messages must be set to English (the encoding does not matter)

  Example usage:

    conn_config = [
      hostname: "localhost",
      username: "user",
      password: "pass",
      database: "db"
    ]
    res = parse_and_replay("postgresql-2019-08-02_140207.log", conn_config)

  """

  @doc """
  Parses postgresql log file and replays it against database.

  For large log files, this can be memory intensive as all the statements are
  parsed into the memory first. Prefer to use `parse_and_replay_via_replayfile/3`.
  For `conn_config` see `t:Postgrex.start_option/0`.
  """
  def parse_and_replay(logfile, conn_config) do
    {parse_time, {statements, stats}} = :timer.tc(&parse/1, [logfile])
    {replay_time, errors} = :timer.tc(&replay/2, [statements, conn_config])

    %{
      parse_time: parse_time,
      replay_time: replay_time,
      stats: stats,
      errors: errors
    }
  end

  @doc """
  Parses postgresql log file into intermediate format. Then replays it against database.

  For `conn_config` see `t:Postgrex.start_option/0`.
  """
  def parse_and_replay_via_replayfile(logfile, replayfile, conn_config) do
    {parse_time, stats} = :timer.tc(&parse_to_replayfile/2, [logfile, replayfile])
    {replay_time, errors} = :timer.tc(&replay_from_replayfile/2, [replayfile, conn_config])

    %{
      parse_time: parse_time,
      replay_time: replay_time,
      stats: stats,
      errors: errors
    }
  end

  @doc """
  Replays statements from replay file against database.

  The replay file can be obtained via `parse_to_replayfile/2`.
  """
  def replay_from_replayfile(replayfile, conn_config) do
    {:ok, conn} = Postgrex.start_link(conn_config)

    File.stream!(replayfile)
    |> Stream.map(&statement_from_encoded_line/1)
    |> Enum.reduce([], fn {statement, params}, errors ->
      case Postgrex.query(conn, statement, params) do
        {:ok, _result} -> errors
        {:error, error} -> [error | errors]
      end
    end)
    |> Enum.reverse()
  end

  @doc """
  Replays statements against database.
  """
  def replay(statements, conn_config) do
    {:ok, conn} = Postgrex.start_link(conn_config)

    Enum.reduce(statements, [], fn {statement, params}, errors ->
      case Postgrex.query(conn, statement, params) do
        {:ok, _result} -> errors
        {:error, error} -> [error | errors]
      end
    end)
    |> Enum.reverse()
  end

  @doc """
  Parses statements from logfile.

  Returns the statements and statistics.
  """
  def parse(logfile) do
    acc_fun = fn statement, acc -> [statement | acc] end
    {statements, stats} = parse(logfile, [], acc_fun)
    {Enum.reverse(statements), stats}
  end

  @doc """
  Parses statements from logfile and saves them into replayfile.

  Returns the statistics.
  """
  def parse_to_replayfile(logfile, replayfile) do
    {:ok, {_fh, stats}} =
      File.open(replayfile, [:write], fn replay_fh ->
        parse(logfile, replay_fh, fn statement, fh -> statement_to_file(statement, fh) end)
      end)

    stats
  end

  @stats_empty %{
    lines_read: 0,
    total_statements: 0,
    simple_statements: 0,
    parametrized_statements: 0,
    prepared_statements_processed: 0,
    unique_prepared_statements: 0,
    prepared_statements_hash: MapSet.new()
  }

  defp parse(logfile, statement_acc, statement_acc_fun) do
    {_cont_line, statements, stats} =
      logfile
      |> File.stream!()
      |> Enum.reduce(
        {
          nil,
          statement_acc,
          @stats_empty
        },
        fn line, acc ->
          parse_line(line, acc, statement_acc_fun)
        end
      )

    {statements, stats}
  end

  defp statement_to_file(statement, fh) do
    :ok = IO.binwrite(fh, :base64.encode(:erlang.term_to_binary(statement)) <> "\n")
    fh
  end

  defp statement_from_encoded_line(line) do
    :erlang.binary_to_term(:base64.decode(line))
  end

  defp parse_line("\t" <> rest, {incomplete_statement, statement_acc, stats}, _statement_acc_fun)
       when incomplete_statement != nil do
    stats = %{stats | lines_read: stats.lines_read + 1}
    {"#{incomplete_statement} #{rest}", statement_acc, stats}
  end

  defp parse_line(line, {incomplete_statement, statement_acc, stats}, statement_acc_fun) do
    stats = %{stats | lines_read: stats.lines_read + 1}

    case Regex.run(~r/^(.*?)\|(.*?)\|(.*?)\|(.*?)\|(.*?):  (.*)/s, line, capture: :all_but_first) do
      [_time, _user, _db, _session, log_type, rest] ->
        parse_log_entry(
          log_type,
          rest,
          {incomplete_statement, statement_acc, stats},
          statement_acc_fun
        )

      nil ->
        {incomplete_statement, statement_acc, stats}
    end
  end

  def parse_log_entry(
        log_type,
        rest,
        {incomplete_statement, statement_acc, stats},
        statement_acc_fun
      ) do
    rest = String.trim(rest)

    case log_type do
      "DETAIL" ->
        {
          nil,
          statement_acc_fun.({incomplete_statement, parse_detail(rest)}, statement_acc),
          stats
        }

      _ ->
        {statement, stats} = parse_statement(rest, stats)

        statement_acc =
          if incomplete_statement do
            statement_acc_fun.({incomplete_statement, []}, statement_acc)
          else
            statement_acc
          end

        {statement, statement_acc, stats}
    end
  end

  defp parse_statement(statement, acc) do
    cond do
      String.starts_with?(statement, "statement: ") ->
        {
          String.split_at(statement, 11) |> elem(1),
          %{
            acc
            | total_statements: acc.total_statements + 1,
              simple_statements: acc.simple_statements + 1
          }
        }

      String.starts_with?(statement, "execute <unnamed>: ") ->
        {
          String.split_at(statement, 19) |> elem(1),
          %{
            acc
            | total_statements: acc.total_statements + 1,
              parametrized_statements: acc.parametrized_statements + 1
          }
        }

      Regex.match?(~r/^execute .*?: /, statement) ->
        [query_name, statement] =
          Regex.run(~r/^execute (.*?): (.*)/, statement, capture: :all_but_first)

        prepared_statements = MapSet.put(acc.prepared_statements_hash, query_name)

        {
          statement,
          %{
            acc
            | total_statements: acc.total_statements + 1,
              prepared_statements_processed: acc.prepared_statements_processed + 1,
              prepared_statements_hash: prepared_statements,
              unique_prepared_statements: MapSet.size(prepared_statements)
          }
        }

      true ->
        {nil, acc}
    end
  end

  defp parse_detail("parameters: " <> parameters) do
    Regex.split(~r/(, )?\$[1-9][0-9]* = /, parameters, trim: true)
    |> Enum.map(&convert_param/1)
  end

  defp convert_param(param) do
    {:done, converted_param} =
      {nil, param}
      |> maybe(&param_to_null/1)
      |> maybe(&param_trim/1)
      |> maybe(&param_to_boolean/1)
      |> maybe(&param_to_integer/1)
      |> maybe(&param_to_datetime/1)
      |> maybe(&param_to_naivedatetime/1)
      |> maybe(&param_to_list/1)
      |> maybe(&param_to_string/1)

    converted_param
  end

  defp maybe({nil, arg}, fun) do
    fun.(arg)
  end

  defp maybe({:done, arg}, _fun) do
    {:done, arg}
  end

  defp param_to_null(param) do
    case param do
      "NULL" ->
        {:done, nil}

      _ ->
        {nil, param}
    end
  end

  defp param_trim(param) do
    {nil, String.trim(param, "'")}
  end

  defp param_to_boolean(param) do
    case param do
      "f" -> {:done, false}
      "t" -> {:done, true}
      _ -> {nil, param}
    end
  end

  defp param_to_integer(param) do
    try do
      {:done, String.to_integer(param)}
    rescue
      ArgumentError -> {nil, param}
    end
  end

  defp param_to_datetime(param) do
    case DateTime.from_iso8601(param) do
      {:ok, datetime, _offset} ->
        {:done, datetime}

      {:error, _} ->
        {nil, param}
    end
  end

  defp param_to_naivedatetime(param) do
    case NaiveDateTime.from_iso8601(param) do
      {:ok, datetime} ->
        {:done, datetime}

      {:error, _} ->
        {nil, param}
    end
  end

  defp param_to_list(param) do
    if String.starts_with?(param, "{") do
      {:done,
       param
       |> String.slice(1, String.length(param) - 2)
       |> String.split(",")}
    else
      {nil, param}
    end
  end

  defp param_to_string(param) do
    {:done, param}
  end
end
