# Publisher Bridge Template for ROS2

This repository contains a template for bridging a ROS2 publisher with a make87 subscriber.
It takes care of converting from make87's proto-based messages to ROS2's CDR message types, as well as establishing
network communication with ROS2's DDS middleware.

## How it works

The application creates a make87 subscriber that listens to the ROS2 publisher topic that should be bridged.
It then converts the received ROS2 messages to make87 messages and publishes them on a make87 topic to the make87
subscriber.
