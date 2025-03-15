<?php

use Illuminate\Support\Facades\Route;
use App\Http\Controllers\HomeController;

Route::controller(HomeController::class)->group(function () {
    Route::get('/', 'index')->name('home');
    Route::get('/demo', 'demo')->name('demo');
    Route::post('process-from', 'askOpenRouter')->name('processForm');
});

