defmodule PgxreplayTest do
  use ExUnit.Case
  doctest Pgxreplay

  test "stderr log parsing" do
    {_statements, stats} = Pgxreplay.parse("#{__DIR__}/errlog")

    assert stats.lines_read == 175
    assert stats.total_statements == 22
    assert stats.simple_statements == 16
    assert stats.parametrized_statements == 4
    assert stats.prepared_statements_processed == 2
    assert stats.unique_prepared_statements == 1
  end
end
