#include <string>

std::string greet(const std::string& name) { return "hello, " + name; }

class Farewell {
public:
    std::string say(const std::string& name) const { return "bye, " + name; }
};
