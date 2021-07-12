#include <atomic>
#include <cinttypes>
#include <cstring>
#include <memory>

#include "leveldb/cache.h"

#include "pthread.h"
#include "random"
#include "sys/time.h"

std::string cache_name = "leveldb";
double FLAGS_factor = 1.0;
double FLAGS_hit_rate = 0.9;            // Control hit rate of the cache.
uint32_t FLAGS_cache_size = 10240;       // Number of bytes to use as a cache of uncompressed data.
uint32_t FLAGS_threads = 16;            // Number of concurrent threads to run.
uint32_t FLAGS_num_shard_bits = 6;      // shard_bits.
uint32_t FLAGS_ops_per_thread = 0;      // Number of operations per thread. (Default: 5 * keyspace size)
uint32_t FLAGS_skew = 5;                // Degree of skew in key selection
constexpr uint32_t FLAGS_value_bytes = 8;         // Size of each value added.
constexpr uint32_t FLAGS_key_size = 16;
uint32_t FLAGS_op_num = 1024000;
bool FLAGS_populate_cache = true;       // Populate cache before operations



namespace {
class Slice {
 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) {}

  // Create a slice that refers to d[0,n-1].
  Slice(const char *d, size_t n) : data_(d), size_(n) {}

  // Create a slice that refers to the contents of "s"
  Slice(const std::string &s) : data_(s.data()), size_(s.size()) {}

  // Create a slice that refers to s[0,strlen(s)-1]
  Slice(const char *s) : data_(s), size_(strlen(s)) {}

  // Intentionally copyable.
  // deep copy
  Slice(const Slice &);

  // deep copy
  Slice &operator=(const Slice &) = default;

  bool operator<(const Slice &r) const;

  // Return a pointer to the beginning of the referenced data
  const char *data() const { return data_; }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() {
    data_ = "";
    size_ = 0;
  }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  std::string ToString() const { return std::string(data_, size_); }

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice &b) const;

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice &x) const {
    return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
  }

 private:
  const char *data_;
  size_t size_;
};

inline bool operator==(const Slice &x, const Slice &y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice &x, const Slice &y) { return !(x == y); }

inline int Slice::compare(const Slice &b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_)
      r = -1;
    else if (size_ > b.size_)
      r = +1;
  }
  return r;
}

bool Slice::operator<(const Slice &r) const {
  if (compare(r) < 0) return true;
  return false;
}

Slice::Slice(const Slice &t) {
  size_ = t.size();
  data_ = (char *) malloc(size_);
  memcpy((void *) data_, t.data(), size_);
}


namespace port {

constexpr bool kLittleEndian = true;

static inline int64_t GetUnixTimeUs() {
  struct timeval tp;
  gettimeofday(&tp, nullptr);
  return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
}

static int PthreadCall(const char *label, int result) {
  if (result != 0 && result != ETIMEDOUT) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
  return result;
}

class Mutex;

class CondVar {
 public:
  explicit CondVar(Mutex *mu);

  ~CondVar();

  void Wait();

  // Timed condition wait.  Returns true if timeout occurred.
  bool TimedWait(uint64_t abs_time_us);

  void Signal();

  void SignalAll();

 private:
  pthread_cond_t cv_;
  Mutex *mu_;
};


class Mutex {
 public:
  explicit Mutex(bool adaptive = false);

  // No copying
  Mutex(const Mutex &) = delete;

  void operator=(const Mutex &) = delete;

  ~Mutex();

  void Lock();

  void Unlock();

  // this will assert if the mutex is not locked
  // it does NOT verify that mutex is held by a calling thread
  void AssertHeld();

 private:
  friend class CondVar;

  pthread_mutex_t mu_;
#ifndef NDEBUG
  bool locked_ = false;
#endif
};

class MutexLock {
 public:
  explicit MutexLock(port::Mutex *mu) : mu_(mu) {
    this->mu_->Lock();
  }

  // No copying allowed
  MutexLock(const MutexLock &) = delete;

  void operator=(const MutexLock &) = delete;

  ~MutexLock() { this->mu_->Unlock(); }

 private:
  port::Mutex *const mu_;
};


Mutex::Mutex(bool adaptive) {
  (void) adaptive;
#ifdef ROCKSDB_PTHREAD_ADAPTIVE_MUTEX
  if (!adaptive) {
    PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr));
  } else {
    pthread_mutexattr_t mutex_attr;
    PthreadCall("init mutex attr", pthread_mutexattr_init(&mutex_attr));
    PthreadCall("set mutex attr",
                pthread_mutexattr_settype(&mutex_attr,
                                          PTHREAD_MUTEX_ADAPTIVE_NP));
    PthreadCall("init mutex", pthread_mutex_init(&mu_, &mutex_attr));
    PthreadCall("destroy mutex attr",
                pthread_mutexattr_destroy(&mutex_attr));
  }
#else
  PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr));
#endif // ROCKSDB_PTHREAD_ADAPTIVE_MUTEX
}

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() {
  PthreadCall("lock", pthread_mutex_lock(&mu_));
#ifndef NDEBUG
  locked_ = true;
#endif
}

void Mutex::Unlock() {
#ifndef NDEBUG
  locked_ = false;
#endif
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void Mutex::AssertHeld() {
#ifndef NDEBUG
  assert(locked_);
#endif
}

CondVar::CondVar(Mutex *mu)
    : mu_(mu) {
  PthreadCall("init cv", pthread_cond_init(&cv_, nullptr));
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() {
#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
}

bool CondVar::TimedWait(uint64_t abs_time_us) {
  struct timespec ts;
  ts.tv_sec = static_cast<time_t>(abs_time_us / 1000000);
  ts.tv_nsec = static_cast<suseconds_t>((abs_time_us % 1000000) * 1000);

#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  int err = pthread_cond_timedwait(&cv_, &mu_->mu_, &ts);
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
  if (err == ETIMEDOUT) {
    return true;
  }
  if (err != 0) {
    PthreadCall("timedwait", err);
  }
  return false;
}

void CondVar::Signal() {
  PthreadCall("signal", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&cv_));
}

}

inline void EncodeFixed64(char *buf, uint64_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

class Random64 {
 private:
  std::mt19937_64 generator_;
 public:
  explicit Random64(uint64_t s) : generator_(s) {}

  // Generates the next random number
  uint64_t Next() { return generator_(); }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint64_t Uniform(uint64_t n) {
    return std::uniform_int_distribution<uint64_t>(0, n - 1)(generator_);
  }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(uint64_t n) { return Uniform(n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint64_t Skewed(int max_log) {
    return Uniform(uint64_t(1) << Uniform(max_log + 1));
  }
};

}

namespace cache_test {

class CacheBench;

// State shared by all concurrent executions of the same benchmark.
class SharedState {
 public:
  explicit SharedState(CacheBench *cache_bench)
      : cv_(&mu_),
        num_initialized_(0),
        start_(false),
        num_done_(0),
        cache_bench_(cache_bench){}

  ~SharedState() {}

  port::Mutex *GetMutex() {
    return &mu_;
  }

  port::CondVar *GetCondVar() {
    return &cv_;
  }

  CacheBench *GetCacheBench() const {
    return cache_bench_;
  }

  void IncInitialized() {
    num_initialized_++;
  }

  void IncDone() {
    num_done_++;
  }

  bool AllInitialized() const { return num_initialized_ >= FLAGS_threads; }

  bool AllDone() const { return num_done_ >= FLAGS_threads; }

  void SetStart() {
    start_ = true;
  }

  bool Started() const {
    return start_;
  }

 private:
  port::Mutex mu_;
  port::CondVar cv_;

  uint64_t num_initialized_;
  bool start_;
  uint64_t num_done_;

  CacheBench *cache_bench_;
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  uint32_t tid;
  Random64 rnd;
  SharedState *shared;
  uint64_t miss_;

  void IncMiss() {
    miss_++;
  }

  uint64_t GetMissNum() const{
    return miss_;
  }

  ThreadState(uint32_t index, SharedState *_shared)
      : tid(index), rnd(1000 + index), shared(_shared), miss_(0) {}
};

struct KeyGen {
  static constexpr int data_len = 20;
  char key_data[data_len];

  Slice GetRand(Random64 &rnd, uint64_t max_key) {
    uint64_t raw = rnd.Next();

    uint64_t key = raw % max_key; // TODO: 设置key的范围
    size_t len = 0;
    for (int i = 1; i <= FLAGS_key_size; ++i) {
      key_data[data_len-i] = '0' + key % 10;
      key /= 10;
    }
    return Slice(key_data+data_len-FLAGS_key_size, FLAGS_key_size);
  }
};

char *createValue(Random64 &rnd) {
  char *rv = new char[FLAGS_value_bytes];
  // Fill with some filler data, and take some CPU time
  for (uint32_t i = 0; i < FLAGS_value_bytes; i += 8) {
    EncodeFixed64(rv + i, rnd.Next());
  }
  return rv;
}

void deleter(const leveldb::Slice &key, void *value) {
  fprintf(stdout, "delete handle key=%s", key.data());
  delete[] static_cast<char *>(value);
}

static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }

class TestCache {
 public:
  TestCache(size_t size):size_(size), cnt_(0), cache_(leveldb::NewLRUCache(size*FLAGS_factor)){}
  bool Get(uint64_t key){
    char key_data[FLAGS_key_size];
    for (int i = 1; i <= FLAGS_key_size; ++i) {
      key_data[FLAGS_key_size-i] = '0' + (key % 10);
      key /= 10;
    }
    leveldb::Cache::Handle* handle = cache_->Lookup(leveldb::Slice(key_data, FLAGS_key_size));
    if (handle != nullptr) {
      cache_->Release(handle);
      return true;
    }
    return false;
  }

  bool Set(uint64_t key){
    char key_data[FLAGS_key_size];
    for (int i = 1; i <= FLAGS_key_size; ++i) {
      key_data[FLAGS_key_size-i] = '0' + (key % 10);
      key /= 10;
    }
    auto k = leveldb::Slice(key_data, FLAGS_key_size);
    auto handle = cache_->Insert(k, nullptr, 1, &deleter);
    if(handle == nullptr){
      cache_->Release(handle);
      return false;
    }

    cnt_++;
    return true;
  }

  void Eliminate(){
    cnt_--;
  }

  size_t GetSize(){
    return size_;
  }

  ~TestCache(){}

  size_t size_;
  size_t cnt_;
  leveldb::Cache* cache_;
};

class CacheBench {

 public:
  CacheBench() : max_key_(0){
    max_key_ = FLAGS_cache_size * 10 / FLAGS_hit_rate;
    // FIXME: choose cache
    cache_ = std::make_shared<TestCache>(FLAGS_cache_size);

    if (FLAGS_ops_per_thread == 0) {
      FLAGS_ops_per_thread = max_key_ * 5;
//      FLAGS_ops_per_thread = FLAGS_op_num / FLAGS_threads;
    }
  }

  ~CacheBench() {}

  void PopulateCache() {
    Random64 rnd(1);
    KeyGen keygen;
    for (uint64_t i = 0; i < FLAGS_cache_size; i++) {
//      Slice key = keygen.GetRand(rnd, max_key_);
      cache_->Set(i);
    }
  }

  bool Run() {
//    PrintEnv();
    SharedState shared(this);
    std::vector<std::unique_ptr<ThreadState> > threads(FLAGS_threads);
    for (uint32_t i = 0; i < FLAGS_threads; i++) {
      threads[i].reset(new ThreadState(i, &shared));
      pthread_t t;
      pthread_create(&t, nullptr, reinterpret_cast<void *(*)(void *)>(ThreadBody), (void*)threads[i].get());
//      std::thread(&ThreadBody, threads[i].get());
    }
    {
      port::MutexLock l(shared.GetMutex());
      while (!shared.AllInitialized()) {
        shared.GetCondVar()->Wait();
      }
      // Record start time
      uint64_t start_time = port::GetUnixTimeUs();

      // Start all threads
      shared.SetStart();
      shared.GetCondVar()->SignalAll();

      // Wait threads to complete
      while (!shared.AllDone()) {
        shared.GetCondVar()->Wait();
      }

      // Record end time
      uint64_t end_time = port::GetUnixTimeUs();
      double elapsed = static_cast<double>(end_time - start_time) * 1e-6;
      uint32_t qps = static_cast<uint32_t>(
          static_cast<double>(FLAGS_threads * FLAGS_ops_per_thread) / elapsed);
      uint64_t miss = 0;
      for(auto &t:threads){
        miss += t->GetMissNum();
      }
      fprintf(stdout, "|%s|%" PRIu32 "|%" PRIu32 "|%" PRIu32 "|%" PRIu32 "|%f|%u|%f|\n",
              cache_name.c_str(), max_key_, FLAGS_cache_size,
              FLAGS_threads, FLAGS_ops_per_thread*FLAGS_threads, elapsed, qps,
              1-miss*1.0/(FLAGS_ops_per_thread*FLAGS_threads));
    }
    return true;
  }

  void PrintEnv() const {
    printf("Number of threads   : %u\n", FLAGS_threads);
    printf("Ops per thread      : %" PRIu32 "\n", FLAGS_ops_per_thread);
    printf("Cache size          : %" PRIu32 "\n", FLAGS_cache_size);
    printf("Num shard bits      : %u\n", FLAGS_num_shard_bits);
    printf("Max key             : %" PRIu32 "\n", max_key_);
    printf("Skew degree         : %u\n", FLAGS_skew);
    printf("Populate cache      : %d\n", int{FLAGS_populate_cache});
    printf("----------------------------\n");
  }

 private:
  std::shared_ptr<TestCache> cache_;
  uint32_t max_key_;

  static void ThreadBody(void *v) {
    ThreadState *thread = static_cast<ThreadState *>(v);
    SharedState *shared = thread->shared;

    {
      port::MutexLock l(shared->GetMutex());
      shared->IncInitialized();
      if (shared->AllInitialized()) {
        shared->GetCondVar()->SignalAll();
      }
      while (!shared->Started()) {
        shared->GetCondVar()->Wait();
      }
    }
    thread->shared->GetCacheBench()->OperateCache(thread);

    {
      port::MutexLock l(shared->GetMutex());
      shared->IncDone();
      if (shared->AllDone()) {
        shared->GetCondVar()->SignalAll();
      }
    }
  }

  void OperateCache(ThreadState *thread) {

    for (uint64_t i = 0; i < FLAGS_ops_per_thread; i++) {
//      Slice key = gen.GetRand(thread->rnd, max_key_);
      uint64_t key = thread->rnd.Next() % max_key_;

      bool ans = cache_->Get(key);
      if(!ans){
        thread->IncMiss();
        cache_->Set(key);
      }
    }
  }

};

}  // namespace test_cache

void TestCase(){
  cache_test::CacheBench bench;
//  bench.PrintEnv();
  if (FLAGS_populate_cache) {
    bench.PopulateCache();
  }
  bench.Run();
}

bool MyCacheTest(){
  cache_test::TestCache *c = new cache_test::TestCache(1000);
  Slice value;
  assert(c->Set(1) == true);
//  assert(c->Set(3) == true);
//  assert(c->GetSize() == 1);
  assert(c->Get(1) == true);
  assert(c->Get(2) == false);
  fprintf(stdout, "passed test\n");
  delete c;
  return true;
}

int main(int argc, char **argv) {
  assert(MyCacheTest());
  // 测试相同命中率不通线程数下的性能
//  FLAGS_hit_rate = 1;
//  FLAGS_factor = 30;
//  for(int i=1;i<600;i<<=1){
//    FLAGS_threads = i;
//    TestCase();
//    FLAGS_ops_per_thread = 0;
//  }

  // 测试单线程下不通命中率下性能
  FLAGS_threads = 1;
  FLAGS_cache_size = 1024;
  FLAGS_ops_per_thread = 256000;
  for (double i = 0.2; i <= 1; i+=0.2) {
//    FLAGS_factor = i;
    FLAGS_hit_rate = i;
    TestCase();
  }
  return 0;
}
