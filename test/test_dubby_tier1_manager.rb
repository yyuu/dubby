require 'helper'

class TestDubbyTier1StoreManager < Test::Unit::TestCase
  def setup()
    options = {
      :protocol => 'hash',
      :serializer => 'yaml',
      :manager => 'tier1',
    }
    @store = DubbyStore.new(options)
    @store_manager = DubbyTier1StoreManager.new(@store, options)
  end
  def test_get_failure()
    key = "test_get_failure"
    val = "value of #{key}"
    assert_raise(DubbyStoreManager::InvalidKeyError) {
      @store_manager.get(key)
    }
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
    assert_raise(DubbyStoreManager::InvalidKeyError) {
      @store_manager.get(key)
    }
  end
  def test_uncommitted_set_and_delete2()
    key = "test_uncommitted_set_and_delete2"
    val = "value of #{key}"
    @store_manager.set(key, val)
    @store_manager.delete!(key)
    assert_raise(DubbyStoreManager::InvalidKeyError) {
      @store_manager.get(key)
    }
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
    assert_raise(DubbyStoreManager::InvalidKeyError) {
      @store_manager.get(key)
    }
  end
  def test_committed_set_and_delete2()
    key = "test_committed_set_and_delete2"
    val = "value of #{key}"
    @store_manager.set!(key, val)
    @store_manager.delete!(key)
    assert_raise(DubbyStoreManager::InvalidKeyError) {
      @store_manager.get(key)
    }
  end
end
