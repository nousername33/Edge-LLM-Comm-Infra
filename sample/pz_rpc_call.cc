#include <iostream>
#include "pzmq.hpp"
#include "pzmq_data.h"
#include <string>

using namespace StackFlows;

int main(int argc, char* argv[])
{
    pzmq _rpc("test");

    _rpc.call_rpc_action(
        "fun1", pzmq_data::set_param("bilibili", "sorbai"), [](pzmq* self, const std::shared_ptr<pzmq_data>& msg) {
            std::string raw_msg = msg->string();
            // 打印原始数据的十六进制表示
            std::cout << "Raw data (hex): ";
            for (char c : raw_msg) {
                printf("%02X ", static_cast<unsigned char>(c));
            }
            std::cout << std::endl;
            
            std::string response_param0 = msg->get_param(0);
            std::string response_param1 = msg->get_param(1);

            std::cout << "Response from fun1: " << response_param0 << ", " << response_param1 << std::endl;
        });

    return 0;
}