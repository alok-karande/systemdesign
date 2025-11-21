# Consistent Hashing with LRU Cache

This simple project implements Consistent Hashing with one or more nodes (and associated virtual nodes). Each Virtual node is an instance of an LRU Cache (Least Recently used).

I'm jumping the gun here. Here's a quick dump on Consistent Hashing and LRU Caches.

## Consistent Hashing

Consistent Hashing is an algorithm/technique that efficiently redestributes data when adding or removing an instance in a distributed system. A simple technique like *Simple Module Hashing* could be used to assign 


## LRU Cache


## Setup

### Prerequisites

**Note:** This setup is specifically written for and tested on a MAC, but will work with appropriate updates for Windows/Linux as well. I will leave that to you. 

## 1. Install Docker

I use docker to deploy multiple instances of the LRU cache and manage them through consistent hashing. 
On MAC, install Docker Desktop using this link: https://docs.docker.com/desktop/setup/install/mac-install/

Once installed, run the Docker Desktop app.

## 2. Install python3 and necessary packages

This code is written in python. Download and install the latest stable version of python3.
Install the following packages using pip3:

*docker, 
flask*

## 3. Update Environmental variables

The following environmental variables should be specified as required:

**CACHE_SIZE**: This sets the size of the Cache for each of the Cache Nodes. 

***SERVERS***: Comma separated list of servers that are part of the consistent hash ring.

***REPLICATION_FACTOR***: The replication factor determines how many virtual nodes are to be added. Specifying a value of 2 would mean one Server instance + one Virtual node instance. Virtual modes are used to uniformly distribute the load on one server. 


## 4. Testing

### Option 1: Local Python testing

### Option 2: Testing wih docker containerized Cache nodes



