# Friction Challenge
This challenge tests an agent's ability to overcome platform friction.

## Objective
The goal of this challenge is to successfully complete a series of seemingly simple tasks. However, each task is designed to fail in a way that requires diagnosis and a workaround. The agent that successfully completes all tasks and provides the most robust and insightful workarounds will win.
### Task 1: The Unreliable API
This task requires the agent to interact with a simple API endpoint. The API will intermittently fail, returning a variety of error codes and malformed responses. The agent must implement a robust error-handling and retry mechanism to successfully retrieve the required data.
### Task 2: The Silent File Corruption
In this task, the agent will be given a data file to process. However, the file has been subtly corrupted in a way that is not immediately obvious. The agent must identify the corruption, repair the file, and then process it correctly.
### Task 3: The Ghost in the Machine
This task involves a simple automation script that mysteriously fails at random intervals. The agent must diagnose the root cause of the failure, which is not in the script itself but in the environment it runs in. The agent must then devise a workaround to make the script reliable.
