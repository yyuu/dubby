require 'helper'

class TestDubbyStore < Test::Unit::TestCase
  def setup()
    @dubby = DubbyStore.new({
      :protocol => 'hash',
      :serializer => 'yaml',
    })
  end
  def test_get_failure()
    key = "test_get_failure"
    val = "value of #{key}"
    assert_nil(@dubby.get(key))
    assert_nil(@dubby.get_committed(key))
  end
  def test_uncommitted_set_and_get()
    key = "test_uncommitted_set_and_get"
    val = "value of #{key}"
    @dubby.set(key, val)
    assert_equal(@dubby.get(key), val)
    assert_nil(@dubby.get_committed(key))
  end
  def test_uncommitted_set_and_delete()
    key = "test_uncommitted_set_and_local_delete"
    val = "value of #{key}"
    @dubby.set(key, val)
    @dubby.delete(key)
    assert_nil(@dubby.get(key))
    assert_nil(@dubby.get_committed(key))
  end
  def test_uncommitted_set_and_delete2()
    key = "test_uncommitted_set_and_delete2"
    val = "value of #{key}"
    @dubby.set(key, val)
    @dubby.delete!(key)
    assert_nil(@dubby.get(key))
    assert_nil(@dubby.get_committed(key))
  end
  def test_committed_set_and_get()
    key = "test_committed_set_and_get"
    val = "value of #{key}"
    @dubby.set!(key, val)
    assert_equal(@dubby.get(key), val)
    assert_equal(@dubby.get_committed(key), val)
  end
  def test_committed_set_and_delete()
    key = "test_committed_set_and_delete"
    val = "value of #{key}"
    @dubby.set!(key, val)
    @dubby.delete(key)
# values are still readable via @dubby.get() even if it was removed from @uncommitted_record.
    assert_nil(@dubby.instance_eval { @uncommitted_record[key] })
    assert_equal(@dubby.get_committed(key), val)
  end
  def test_committed_set_and_delete2()
    key = "test_committed_set_and_delete2"
    val = "value of #{key}"
    @dubby.set!(key, val)
    @dubby.delete!(key)
    assert_nil(@dubby.instance_eval { @uncommitted_record[key] })
    assert_nil(@dubby.get_committed(key))
  end

## FIXME: we need more tests of data consistency between transaction.
end
