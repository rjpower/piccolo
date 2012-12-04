#include "piccolo.h"

using namespace std;
using namespace piccolo;

DEFINE_string(book_source, "/home/yavcular/books/520.txt", "");

static TableT<string, string>* books;
static TableT<string, int>* counts;

struct WordCount {
  void setup(const ConfigData& conf) {
    counts = TableRegistry::sparse(1, new Sharding::String,
        new Accumulators<int>::Sum);

    // books = TableRegistry::text(FLAGS_book_source);
  }

  void run(Master *m, const ConfigData& conf) {
    m->map(books, [](const string& key, const string& value) {
      vector<StringPiece> words = StringPiece::split(value, " ");
      for (int j = 0; j < words.size(); ++j) {
        words[j].strip();
        counts->update(words[j].AsString(), 1);
      }
    });

    m->map(counts, [](const string& key, int value) {
      if (value > 50) {
        printf("%20s : %d\n", key.c_str(), value);
      }
    });
  }
};
