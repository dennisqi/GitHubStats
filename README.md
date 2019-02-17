# Repo Census

Measuring the history and creation of GitHub repositories.

[Link](https://docs.google.com/presentation/d/1t3t8k1MA05ME9Y2PLC_HunlVk81LljYxHzfrMuerPkM/edit#slide=id.g33ca9faa1d_4_0) to the presentation.

<hr/>

## Introduction
GitHub is the largest open source community in the world. My project aims to measure and visualize the number of new repositories on GitHub.

In January 2019, GitHub allow users to create free private repositories. Will this decision be reflected on the number of public repo creations?

## Dataset
[GitHub Archive](https://www.gharchive.org)
They have archives of GitHub activities, range from new commits and fork events, to opening new tickets, commenting, and adding members to a project.

These events are aggregated into hourly archives, which you can access with any HTTP client:

|Query|Command|
|-----|-------|
|Activity for 1/1/2015 @ 3PM UTC|`wget http://data.gharchive.org/2015-01-01-15.json.gz`|
|Activity for 1/1/2015|`wget http://data.gharchive.org/2015-01-01-{0..23}.json.gz`|
|Activity for all of January 2015|`wget http://data.gharchive.org/2015-01-{01..31}-{0..23}.json.gz`|


## Architecture
![Pipeline](https://i.imgur.com/MSvBy0N.png)

* Download and decompressed JSON files from GHArchive write into AWS S3.
* Spark reads JSON files into Spark Dataframes and calculates the result
* Then Spark writes result into Postgres on AWS RDS.
* Front-end reads from Postgres and plot chars using Dash by Plotly.

<hr/>

## How to install and get it up and running
