#include "user.h"
#include "database.h"
#include "../config/config.h"

#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/RecordSet.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <sstream>
#include <exception>
#include <algorithm>
#include <future>

using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;

namespace database
{

    void User::init()
    {
        try
        {

            Poco::Data::Session session = database::Database::get().create_session();

            long last_shard_id = 0;
            for (auto &hint : Database::get_all_hints())
            {
                Statement create_stmt(session);
                create_stmt << "CREATE TABLE IF NOT EXISTS `user` ("
                            << "`id` INT NOT NULL AUTO_INCREMENT,"
                            << "`first_name` VARCHAR(256) NOT NULL,"
                            << "`last_name` VARCHAR(256) NOT NULL,"
                            << "`login` VARCHAR(256) NOT NULL,"
                            << "`password` VARCHAR(256) NOT NULL,"
                            << "`email` VARCHAR(256) NULL,"
                            << "`phone` VARCHAR(15) NULL,"
                            << "PRIMARY KEY (`id`),KEY `fn` (`first_name`),KEY `ln` (`last_name`));"
                            << hint,
                    now;
                create_stmt.execute();

                Poco::Data::Statement select(session);
                long temp_id = 0;
                std::string sql = "SELECT `id` FROM `user` ORDER BY `id` DESC LIMIT 0, 1 ";
                sql += hint;
                select << sql,
                    into(temp_id),
                    range(0, 1); //  iterate over result set one row at a time

                select.execute();
                if (last_shard_id < temp_id)
                {
                    last_shard_id = temp_id;
                }
            }
            _last_id = last_shard_id;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.displayText() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.displayText() << std::endl;
            throw;
        }
    }

    Poco::JSON::Object::Ptr User::toJSON() const
    {
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();

        root->set("id", _id);
        root->set("first_name", _first_name);
        root->set("last_name", _last_name);
        root->set("email", _email);
        root->set("phone", _phone);
        root->set("login", _login);
        root->set("password", _password);

        return root;
    }

    User User::fromJSON(const std::string &str)
    {
        User user;
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(str);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        user.id() = object->getValue<long>("id");
        user.first_name() = object->getValue<std::string>("first_name");
        user.last_name() = object->getValue<std::string>("last_name");
        user.email() = object->getValue<std::string>("email");
        user.phone() = object->getValue<std::string>("phone");
        user.login() = object->getValue<std::string>("login");
        user.password() = object->getValue<std::string>("password");

        return user;
    }

    std::optional<User> User::read_by_id(long id)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);
            User a;
            auto hint = Database::sharding_user(id);
            select << "SELECT `id`, `first_name`, `last_name`, `email`, `phone`, `login`, `password`"
                   << "FROM `user`"
                   << "WHERE `id`=?"
                   << hint,
                into(a._id),
                into(a._first_name),
                into(a._last_name),
                into(a._email),
                into(a._phone),
                into(a._login),
                into(a._password),
                use(id),
                range(0, 1); //  iterate over result set one row at a time

            select.execute();
            Poco::Data::RecordSet rs(select);
            if (rs.moveFirst())
                return a;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.displayText() << std::endl;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.displayText() << std::endl;
        }
        return {};
    }

    std::vector<User> User::read_all()
    {
        std::vector<User> result;
        std::vector<std::string> hints = database::Database::get_all_hints();
        std::vector<std::future<std::vector<User>>> futures;

        for (const std::string &hint : hints)
        {
            auto handle = std::async(std::launch::async, [hint]() -> std::vector<User>
                                     {
            try
            {
                                            Poco::Data::Session session = database::Database::get().create_session();
            Statement select(session);
            std::vector<User> result;
            User a;
            select  << "SELECT `id`, `first_name`, `last_name`, `email`, `phone`, `login`, `password`" 
                    << "FROM `user`",
                into(a._id),
                into(a._first_name),
                into(a._last_name),
                into(a._email),
                into(a._phone),
                into(a._login),
                into(a._password),
                range(0, 1); //  iterate over result set one row at a time

            while (!select.done())
            {
                if (select.execute())
                    result.push_back(a);
            }
            return result;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.displayText() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.displayText() << std::endl;
            throw;
        }
                                         
                                        return std::vector<User>(); });

            futures.emplace_back(std::move(handle));
        }

        for (std::future<std::vector<User>> &res : futures)
        {
            std::vector<User> v = res.get();
            std::copy(std::begin(v),
                      std::end(v),
                      std::back_inserter(result));
        }
        std::sort(std::begin(result), std::end(result), [](User a, User b)
                  { return a.get_id() < b.get_id(); });

        return result;
    }

    std::vector<User> User::search(std::string first_name, std::string last_name)
    {
        std::vector<User> result;

        std::vector<std::string> hints = database::Database::get_all_hints();
        std::vector<std::future<std::vector<User>>> futures;

        for (const std::string &hint : hints)
        {
            auto handle = std::async(std::launch::async, [first_name, last_name, hint]() -> std::vector<User>
                                     {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Statement select(session);
            std::vector<User> result;
            User a;
            auto fn = first_name + "%";
            auto ln = last_name + "%";
            select << "SELECT `id`, `first_name`, `last_name`, `email`, `phone`, `login`, `password`"
                   << "FROM `user`"
                   << "WHERE `first_name` LIKE ? AND `last_name` LIKE ?",
                into(a._id),
                into(a._first_name),
                into(a._last_name),
                into(a._email),
                into(a._phone),
                into(a._login),
                into(a._password),
                use(fn),
                use(ln),
                range(0, 1); //  iterate over result set one row at a time

            while (!select.done())
            {
                if (select.execute())
                    result.push_back(a);
            }
            return result;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.displayText() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.displayText() << std::endl;
            throw;
        }

        return std::vector<User>(); });

            futures.emplace_back(std::move(handle));
        }
        for (std::future<std::vector<User>> &res : futures)
        {
            std::vector<User> v = res.get();
            std::copy(std::begin(v),
                      std::end(v),
                      std::back_inserter(result));
        }
        std::sort(std::begin(result), std::end(result), [](User a, User b)
                  { return a.get_id() < b.get_id(); });

        return result;
    }

    bool User::save_to_mysql()
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement insert(session);
            _id = this->get_last_id();
            auto hint = Database::sharding_user(_id);

            insert << "INSERT INTO `user` (`id`, `first_name`, `last_name`, `email`, `phone`, `login`, `password`)"
                   << "VALUES(?, ?, ?, ?, ?, ?, ?)"
                   << hint,
                use(_id),
                use(_first_name),
                use(_last_name),
                use(_email),
                use(_phone),
                use(_login),
                use(_password);

            insert.execute();

            std::cout << "inserted:" << _id << std::endl;
            return true;
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.displayText() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.displayText() << std::endl;
            throw;
        }
    }

    const long &User::get_last_id()
    {
        return ++(_last_id);
    }

    const std::string &User::get_login() const
    {
        return _login;
    }

    const std::string &User::get_password() const
    {
        return _password;
    }

    std::string &User::login()
    {
        return _login;
    }

    std::string &User::password()
    {
        return _password;
    }

    long User::get_id() const
    {
        return _id;
    }

    const std::string &User::get_first_name() const
    {
        return _first_name;
    }

    const std::string &User::get_last_name() const
    {
        return _last_name;
    }

    const std::string &User::get_email() const
    {
        return _email;
    }

    const std::string &User::get_phone() const
    {
        return _phone;
    }

    long &User::id()
    {
        return _id;
    }

    std::string &User::first_name()
    {
        return _first_name;
    }

    std::string &User::last_name()
    {
        return _last_name;
    }

    std::string &User::email()
    {
        return _email;
    }

    std::string &User::phone()
    {
        return _phone;
    }
}