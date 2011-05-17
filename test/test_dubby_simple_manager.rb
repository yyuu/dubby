require 'helper'

class TestDubbySimpleStoreManager < Test::Unit::TestCase
  def setup()
    options = {
      :protocol => 'hash',
      :serializer => 'yaml',
      :manager => 'simple',
    }
    @store = DubbyStore.new(options)
    @store_manager = DubbySimpleStoreManager.new(@store, options)
  end
  def test_get_failure()
    key = "test_get_failure"
    val = "value of #{key}"
    assert_nil(@store_manager.get(key))
  end
  def test_uncommitted_set_and_get()
    key = "test_uncommitted_set_and_get"
    val = "value of #{key}"
    @store_manager.set(key, val)
    assert_equal(@store_manager.get(key), val)
  end
  def test_uncommitted_set_and_delete()
    key = "test_uncommitted_set_and_local_delete"
    val = "value of #{key}"
    @store_manager.set(key, val)
    @store_manager.delete(key)
    assert_nil(@store_manager.get(key))
  end
  def test_uncommitted_set_and_delete2()
    key = "test_uncommitted_set_and_delete2"
    val = "value of #{key}"
    @store_manager.set(key, val)
    @store_manager.delete!(key)
    assert_nil(@store_manager.get(key))
  end
  def test_committed_set_and_get()
    key = "test_committed_set_and_get"
    val = "value of #{key}"
    @store_manager.set!(key, val)
    assert_equal(@store_manager.get(key), val)
  end
  def test_committed_set_and_delete()
    key = "test_committed_set_and_delete"
    val = "value of #{key}"
    @store_manager.set!(key, val)
    @store_manager.delete(key)
    assert_equal(@store_manager.get(key), val)
  end
  def test_committed_set_and_delete2()
    key = "test_committed_set_and_delete2"
    val = "value of #{key}"
    @store_manager.set!(key, val)
    @store_manager.delete!(key)
    assert_nil(@store_manager.get(key))
  end
end
