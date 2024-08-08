#include <string>
#include <functional>
#include <iostream>
using namespace std;
int main() {
    hash<string> hash_fn;
    auto basename = "/1G";
    cout << hash_fn(basename) << endl;
    return 0;
}