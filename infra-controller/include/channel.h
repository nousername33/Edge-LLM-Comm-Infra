#pragma once

// #define __cplusplus 1

#include <semaphore.h>
#include <unistd.h>
#include <iostream>
// #define CONFIG_SUPPORTTHREADSAFE 0

#include <string>
#include <list>
#include <functional>
#include <unordered_map>
#include <mutex>
#include <eventpp/eventqueue.h>
#include <thread>
#include <memory>
#include "json.hpp"
#include <regex>
#include "pzmq.hpp"
#include "StackFlowUtil.h"

#define LLM_NO_ERROR std::string("")
#define LLM_NONE std::string("None")
namespace StackFlows
{
    class llm_channel_obj
    {
    private:
        // 通过work_id区分不同的订阅者
        std::unordered_map<int, std::shared_ptr<pzmq>> zmq_; // ZMQ连接池
        // 通过url订阅
        std::atomic<int> zmq_url_index_; // 连接索引
        std::unordered_map<std::string, int> zmq_url_map_;

    public:
        std::string unit_name_; // 单元名称
        bool enoutput_; // 是否启用输出，控制是否将结果发送给用户
        bool enstream_; // 是否启用流式输出，控制是否将结果分多次发送给用户
        std::string request_id_; //当前请求id，rpc请求标识
        std::string work_id_; // 工作id
        std::string inference_url_; // 外部用户推理服务URL，pub/sub
        std::string publisher_url_; // pub给其他的业务节点模块
        std::string output_url_; // 输出给外部用户通信，push/pull
        std::string publisher_url;

        llm_channel_obj(const std::string &_publisher_url, const std::string &inference_url, const std::string &unit_name);
        ~llm_channel_obj();
        inline void set_output(bool flage)
        {
            enoutput_ = flage;
        }
        inline bool get_output()
        {
            return enoutput_;
        }
        inline void set_stream(bool flage)
        {
            enstream_ = flage;
        }
        inline bool get_stream()
        {
            return enstream_;
        }
        void subscriber_event_call(const std::function<void(const std::string &, const std::string &)> &call, pzmq *_pzmq,
                                   const std::shared_ptr<pzmq_data> &raw);
        int subscriber_work_id(const std::string &work_id,
                               const std::function<void(const std::string &, const std::string &)> &call);
        void stop_subscriber_work_id(const std::string &work_id);
        void subscriber(const std::string &zmq_url, const pzmq::msg_callback_fun &call);
        void stop_subscriber(const std::string &zmq_url);
        int send_raw_to_pub(const std::string &raw);
        int send_raw_to_usr(const std::string &raw);
        void set_push_url(const std::string &url);
        void cear_push_url();
        static int send_raw_for_url(const std::string &zmq_url, const std::string &raw);

        int send(const std::string &object, const nlohmann::json &data, const std::string &error_msg,
                 const std::string &work_id = "")
        {
            nlohmann::json out_body;
            out_body["request_id"] = request_id_;
            out_body["work_id"] = work_id.empty() ? work_id_ : work_id;
            out_body["created"] = time(NULL);
            out_body["object"] = object;
            out_body["data"] = data;
            if (error_msg.empty())
            {
                out_body["error"]["code"] = 0;
                out_body["error"]["message"] = "";
            }
            else
                out_body["error"] = error_msg;

            std::string out = out_body.dump();
            out += "\n";

            send_raw_to_pub(out);
            if (enoutput_)
            {
                return send_raw_to_usr(out);
            }
            return 0;
        }
    };
}