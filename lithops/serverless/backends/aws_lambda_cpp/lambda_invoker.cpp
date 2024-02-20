#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/InvokeResult.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <iostream>
#include <fstream>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <stdexcept> // for std::runtime_error
#include <exception> // for std::exception


extern "C" {
    const char* invoke_lambda(const char* functionName, const char* region, const char* accessKey, const char* secretKey, const char* sessionToken, const char* payload);
}

std::string get_current_time() {
    // Get current time as a time_point object
    auto now = std::chrono::system_clock::now();

    // Get the number of seconds since epoch
    auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(now);
    auto epoch_seconds = seconds.time_since_epoch().count();

    // Get the number of milliseconds since the last second
    auto millisec_since_last_sec = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    // Convert to string
    std::stringstream ss;
    ss << epoch_seconds << '.' << std::setfill('0') << std::setw(3) << millisec_since_last_sec.count();

    return ss.str();
}

const char* invoke_lambda(const char* functionName, const char* region, const char* accessKey, const char* secretKey, const char* sessionToken, const char* payload) {
    std::ofstream file;
    file.open("timestamps.txt", std::ios_base::app);
    file << "Start time: " << get_current_time() << '\n';

    try {
        Aws::SDKOptions options;
        Aws::InitAPI(options);

        // Set your AWS credentials and region
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = region;  // Replace with your AWS region
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.verifySSL = true;

        // Replace these values with your AWS credentials and Lambda function details
        Aws::Auth::AWSCredentials credentials(accessKey, secretKey, sessionToken);

        Aws::Lambda::LambdaClient lambdaClient(credentials, clientConfig);

        Aws::Lambda::Model::InvokeRequest invokeRequest;
        invokeRequest.SetFunctionName(functionName);
        invokeRequest.SetInvocationType(Aws::Lambda::Model::InvocationType::Event); // Set to Event for asynchronous invocation
        invokeRequest.SetLogType(Aws::Lambda::Model::LogType::Tail);

        // Convert payload to AWS IOStream
        auto ss = Aws::MakeShared<Aws::StringStream>("ALLOCATION_TAG");
        *ss << payload;
        invokeRequest.SetBody(ss);

        file << "Invocation time: " << get_current_time() << '\n';
        auto outcome = lambdaClient.Invoke(invokeRequest);
        file << "Return time: " << get_current_time() << '\n';

        if (outcome.IsSuccess()) {
            const auto& result = outcome.GetResult();
            int statusCode = result.GetStatusCode();
            // std::cout << "StatusCode: " << statusCode << std::endl;
            

            Aws::ShutdownAPI(options);
            file.close();
            // std::cout << "Lambda function invoked asynchronously" << std::endl;
            return std::to_string(statusCode).c_str();
        } else {
            throw std::runtime_error("Failed to invoke Lambda function: " + outcome.GetError().GetMessage());
        }
    } catch(const std::exception& e) {
        // Log the error
        std::cerr << "Error: " << e.what() << std::endl;
        // Close the file
        file.close();
        // Return the error message
        return strdup(("Error: " + std::string(e.what())).c_str());
    }
}
