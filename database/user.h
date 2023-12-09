#ifndef AUTHOR_H
#define AUTHOR_H

#include <string>
#include <vector>
#include "Poco/JSON/Object.h"
#include <optional>

namespace database
{
    class User{
        private:
            long _id;
            std::string _first_name;
            std::string _last_name;
            std::string _email;
            std::string _phone;
            std::string _login;
            std::string _password;
            static inline long _last_id;

        public:

            static User fromJSON(const std::string & str);

            static const long &get_last_id();

            long             get_id() const;
            const std::string &get_first_name() const;
            const std::string &get_last_name() const;
            const std::string &get_email() const;
            const std::string &get_phone() const;
            const std::string &get_login() const;
            const std::string &get_password() const;

            long&        id();
            std::string &first_name();
            std::string &last_name();
            std::string &email();
            std::string &phone();
            std::string &login();
            std::string &password();

            static void init();
            static std::optional<User> read_by_id(long id, bool cache = true);
            static std::vector<User> read_all();
            static std::vector<User> search(std::string first_name,std::string last_name);
            bool save_to_mysql();

            void save_to_cache();
            static std::optional<User> read_from_cache_by_id(long id);

            Poco::JSON::Object::Ptr toJSON() const;

    };
}

#endif