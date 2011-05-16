require 'helper'

class TestDubbySimpleManager < Test::Unit::TestCase
  def setup()
    options = {
      :protocol => 'hash',
      :serializer => 'yaml',
      :manager => 'simple',
    }
    @store = DubbyStore.new(options)
    @manager = DubbySimpleManager.new(@store, options)
  end
  def test_get_failure()
    key = "test_get_failure"
    val = "value of #{key}"
    assert_nil(@manager.get(key))
  end
  def test_uncommitted_set_and_get()
    key = "test_uncommitted_set_and_get"
    val = "value of #{key}"
    @manager.set(key, val)
    assert_equal(@manager.get(key), val)
  end
  def test_uncommitted_set_and_delete()
    key = "test_uncommitted_set_and_local_delete"
    val = "value of #{key}"
    @manager.set(key, val)
    @manager.delete(key)
    assert_nil(@manager.get(key))
  end
  def test_uncommitted_set_and_delete2()
    key = "test_uncommitted_set_and_delete2"
    val = "value of #{key}"
    @manager.set(key, val)
    @manager.delete!(key)
    assert_nil(@manager.get(key))
  end
  def test_committed_set_and_get()
    key = "test_committed_set_and_get"
    val = "value of #{key}"
    @manager.set!(key, val)
    assert_equal(@manager.get(key), val)
  end
  def test_committed_set_and_delete()
    key = "test_committed_set_and_delete"
    val = "value of #{key}"
    @manager.set!(key, val)
    @manager.delete(key)
    assert_equal(@manager.get(key), val)
  end
  def test_committed_set_and_delete2()
    key = "test_committed_set_and_delete2"
    val = "value of #{key}"
    @manager.set!(key, val)
    @manager.delete!(key)
    assert_nil(@manager.get(key))
  end
end
