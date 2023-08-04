# pinterest-data-pipeline

This project is a work in progress.

## Project Brief

Build the system that Pinterest uses to analyse both historical, and real-time data generated by posts from their users.

Pinterest has world-class machine learning engineering systems. They have billions of user interactions such as image uploads or image clicks which they need to process every day to inform the decisions to make. In this project, I am building a system in the cloud that takes in those events and runs them through two separate pipelines. One for computing real-time metrics (such as profile popularity, which would be used to recommend that profile in real-time), and another for computing metrics that depend on historical data (such as the most popular category this year).

