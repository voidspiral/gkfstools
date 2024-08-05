#include <string>
#include <functional>

extern "C" {

unsigned long Cal_Hash(const char* val) {
    std::string str_val(val);
    std::hash<std::string> hash_fn;
    return hash_fn(str_val);
}
}