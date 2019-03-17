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

## Approach
* First Spark read json files from S3 into DataFrame.
* For each record, according to its schema, decide if it is a public repo creation record.
```json

# repository_create_event:
{"type":"CreateEvent","repo":{"id":3082773,"url":"https://api.github.dev/repos/dHeinemann/What-The-Hex","name":"dHeinemann/What-The-Hex"},"created_at":"2012-01-01T15:00:07Z","payload":{"ref":"v1.00","description":"Colour picker application for Windows","master_branch":"master","ref_type":"tag"},"actor":{"login":"dHeinemann","id":203058,"url":"https://api.github.dev/users/dHeinemann","avatar_url":"https://secure.gravatar.com/avatar/6fbdf223ba1ed10fcb9c79113e0ca45e?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png","gravatar_id":"6fbdf223ba1ed10fcb9c79113e0ca45e"},"public":true,"id":"1508546917"}

# branch_create_event:
{"type":"CreateEvent","repo":{"id":3077372,"url":"https://api.github.dev/repos/yuya-takeyama/algerb","name":"yuya-takeyama/algerb"},"created_at":"2012-01-01T15:07:45Z","payload":{"ref":"v0.0.3","master_branch":"develop","description":"Autoloader Generator for Ruby","ref_type":"tag"},"actor":{"login":"yuya-takeyama","id":241542,"url":"https://api.github.dev/users/yuya-takeyama","avatar_url":"https://secure.gravatar.com/avatar/33685a20ded68e6861bec3c1bd7f0870?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png","gravatar_id":"33685a20ded68e6861bec3c1bd7f0870"},"public":true,"id":"1508547304"}

# push_event:
{"type":"PushEvent","repo":{"id":2095911,"url":"https://api.github.dev/repos/sorlok/Dubious-Dabblings","name":"sorlok/Dubious-Dabblings"},"created_at":"2012-01-01T15:00:07Z","payload":{"ref":"refs/heads/master","push_id":55773547,"commits":[{"sha":"f24cc51a5b3ecb57731c6e5e01dd94f2be0ed7ce","author":{"email":"b834296bd3defa26f3fd13cb0e37fce057e3bb4f@gmail.com","name":"Seth N. Hetu"},"url":"https://api.github.com/repos/sorlok/Dubious-Dabblings/commits/f24cc51a5b3ecb57731c6e5e01dd94f2be0ed7ce","message":"Minor library cleanup. Testing which tiles _should_ be drawn."},{"sha":"f18bdfb46c0ce3cadd96c1ff4b6ef66132c49cd1","author":{"email":"b834296bd3defa26f3fd13cb0e37fce057e3bb4f@gmail.com","name":"Seth N. Hetu"},"url":"https://api.github.com/repos/sorlok/Dubious-Dabblings/commits/f18bdfb46c0ce3cadd96c1ff4b6ef66132c49cd1","message":"Map draws, fps is obscured (but seems to be much slower) and tiles are off by half pixels."}],"head":"f18bdfb46c0ce3cadd96c1ff4b6ef66132c49cd1","size":2},"actor":{"login":"sorlok","id":513512,"url":"https://api.github.dev/users/sorlok","avatar_url":"https://secure.gravatar.com/avatar/2d03df1949a14f50017efcdc0aacadcc?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png","gravatar_id":"2d03df1949a14f50017efcdc0aacadcc"},"public":true,"id":"1508546918"}

# issue_event:
{"org":{"login":"jquery","id":70142,"url":"https://api.github.dev/orgs/jquery","avatar_url":"https://secure.gravatar.com/avatar/6906f317a4733f4379b06c32229ef02f?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-org-420.png","gravatar_id":"6906f317a4733f4379b06c32229ef02f"},"type":"IssuesEvent","repo":{"id":907410,"url":"https://api.github.dev/repos/jquery/jquery-mobile","name":"jquery/jquery-mobile"},"created_at":"2012-01-01T15:00:08Z","payload":{"issue":{"milestone":null,"title":"nested list header includes count bubble count","created_at":"2011-12-25T22:08:50Z","user":{"id":669783,"login":"barbalex","url":"https://api.github.com/users/barbalex","gravatar_id":"423c294fd9bdb6df0b479d73239721e6","avatar_url":"https://secure.gravatar.com/avatar/423c294fd9bdb6df0b479d73239721e6?d=https://a248.e.akamai.net/assets.github.com%2Fimages%2Fgravatars%2Fgravatar-140.png"},"labels":[],"updated_at":"2012-01-01T15:00:05Z","state":"closed","id":2656009,"pull_request":{"patch_url":null,"html_url":null,"diff_url":null},"html_url":"https://github.com/jquery/jquery-mobile/issues/3335","closed_at":"2012-01-01T15:00:05Z","body":"I have nested lists showing count bubbles. It's awesome, thank you for the great work!\r\n\r\nOnly thing that's not nice is that the header shows the count from the count bubble. \r\n\r\nSo my list headers look like this: \"Flora7934\". That should be \"Flora\" instead.\r\n\r\nWould be nice, if it were possible to add a class to mark what part of the text of a list item should be used as title of it's nested list.\r\n\r\nAlex","number":3335,"url":"https://api.github.com/repos/jquery/jquery-mobile/issues/3335","comments":3,"assignee":null},"action":"closed"},"actor":{"login":"barbalex","id":669783,"url":"https://api.github.dev/users/barbalex","avatar_url":"https://secure.gravatar.com/avatar/423c294fd9bdb6df0b479d73239721e6?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png","gravatar_id":"423c294fd9bdb6df0b479d73239721e6"},"public":true,"id":"1508546919"}

```
* Count the total number of repository create events of everyday.
* Calculate two types of growth rate:
    * Week-over-week growth rate
        * Compare Sunday with Sunday and Monday with Monday.
    * Month-over-month growth rate
        * Compare each month with previous month.

<hr/>

## How to install and get it up and running
To run the program, you need to have a Spark running on your machine, then use the following spark submit comment to run it for processing history data.
```
$ cd src/calculate/
$ spark-submit —packages org.postgresql:postgresql:42.2.2 main.py history
```

or
```
$ spark-submit —packages org.postgresql:postgresql:42.2.2 main.py present
```
