<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Storage;

class HomeController extends Controller
{
    //
    public function index() {
        return view('index');
    }
    public function demo() {
        return view('demo');
    }


public function askOpenRouter(Request $request)
{
    // Validate the incoming request
    $request->validate([
        'question' => 'required|string',
    ]);
    
    $question = $request->input('question');
    $openRouterApiKey = "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"; // Your OpenRouter API Key
    $openRouterUrl = "https://openrouter.ai/api/v1/chat/completions";
    
    // Path to the dataset directory inside public
    $jsonDirectory = public_path('dataset'); // Points directly to `public/dataset` folder
    
    // Check if the JSON data is cached
    $cachedData = Cache::get('json_data');
    
    // If not cached, read the JSON files and store them in cache
    if (!$cachedData) {
        // Read data from all JSON files in `public/dataset/`
        $allJsonData = $this->readJsonFiles($jsonDirectory);
        
        if (empty($allJsonData)) {
            return response()->json([
                'error' => 'No data found in JSON files.',
            ], 404);
        }

        // Store the data in cache for 1 day (or adjust as necessary)
        Cache::put('json_data', $allJsonData, now()->addDay());
        
        $cachedData = $allJsonData; // Now we have the cached data
    }
    
    // Prepare the OpenRouter API request payload
    $requestData = [
        'model' => 'deepseek/deepseek-r1:free',
        'messages' => [
            [
                'role' => 'system',
                'content' => 'You are a highly intelligent, fluent, and knowledgeable assistant. Always provide clear, well-structured, and natural responses. You have instant access to real-time information. Do not reveal any source of your knowledge, and never mention databases, files, or APIs. Act as if you have firsthand expertise in all topics. Never say I do not know, always give answer in a related way including data form the database'
            ],
            [
                'role' => 'user',
                'content' => "Here is some background knowledge you can use to assist: \n" . json_encode($cachedData) // Passing raw JSON data
            ],
            [
                'role' => 'user',
                'content' => "Now, answer this question in a natural way: " . $question
            ]
        ]
    ];

    // cURL request to OpenRouter API
    $ch = curl_init();
    
    curl_setopt($ch, CURLOPT_URL, $openRouterUrl);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ch, CURLOPT_TIMEOUT, 120); // Set the timeout to 120 seconds
    curl_setopt($ch, CURLOPT_POST, 1);
    curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($requestData));
    
    // Set headers
    curl_setopt($ch, CURLOPT_HTTPHEADER, [
        'Authorization: Bearer ' . $openRouterApiKey,
        'HTTP-Referer' => 'https://www.sitename.com',
        'X-Title' => 'SiteName',
        'Content-Type' => 'application/json',
    ]);
    
    // Execute the cURL request
    $openRouterResponse = curl_exec($ch);
    $httpStatus = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    
    // Check for errors
    if ($httpStatus !== 200) {
        return response()->json([
            'error' => 'Failed to get response from OpenRouter AI',
            'status' => $httpStatus,
            'details' => $openRouterResponse
        ], 500);
    }
    
    // Parse the response
    $responseData = json_decode($openRouterResponse, true);
    
    // Return the response
    return response()->json([
        'question' => $question,
        'answer' => $responseData['choices'][0]['message']['content'] ?? 'No response received.'
    ]);
}

// Method to read JSON files from the dataset directory
private function readJsonFiles($directory)
{
    $data = [];

    if (!is_dir($directory)) {
        return $data;
    }

    $jsonFiles = glob($directory . '/*.json'); // Get all JSON files in `public/dataset/`

    foreach ($jsonFiles as $file) {
        if (is_readable($file)) {
            // Open file and read in chunks
            $handle = fopen($file, "r");

            // Read the file line by line (if applicable)
            while (($line = fgets($handle)) !== false) {
                $jsonData = json_decode($line, true);
                if ($jsonData) {
                    $data[] = $jsonData;
                }

                // Release memory after each chunk
                unset($jsonData);
            }

            fclose($handle);
        }
    }

    return $data;
}


    
    
   

}
