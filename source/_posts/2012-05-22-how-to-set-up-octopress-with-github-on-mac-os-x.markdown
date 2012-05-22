---
layout: post
title: "How to set up octopress with github (on Mac OS X)"
date: 2012-05-22 21:39
comments: true
categories: 
---

Octopress is a blogging framework for hackers. It supports creating
pages using the markdown style, and deploying the site on github.

## Set up github site
github supports blog hosting in the following way: (See: [Github Pages](http://help.github.com/pages/) )

1. You create a repository named yourname.github.com.
2. You place an index.html in the root directory then commit and push onto the master branch.
3. You are good to go.

Octopress's job, then, is to generate pages and automatically push to the right repo.

## Set up Octopress
First, checkout the octopress code from github:
    git clone git://github.com/imathis/octopress.git eshock.github.com
Then create a new blog:
    cd eshock.github.com
    bundle install
    bundle update
    rake install
After this step, the source and public directories are automatically created, in which
your blog source and generated site content are resided.

## Start blogging
You create a new blog post using:
    rake new_post["Hello World"]
A new blog post is created at $site/source/_posts/{date}-hello-world.markdown.
Edit the source in markdown format, then generate contents using:
    rake generate
You can view the blog using `rake preview', while I strongly recommend that you use
[pow](http://pow.cx/).

## Deploy to github
Run the following command once to configure the git upstream:
    rake setup_github_pages
When asked, the URL you type should be the full git commit url with read/write access, which you may see at the repo's page, e.g.:
    git@github.com:eshock/eshock.github.com.git
After this succeeds, a new branch `source' is created, and the remote origin is changed to point to the URL you entered. Then you can run:
    rake generate
    rake deploy
everytime you want to publish the blog. BTW, don't forget to upload the source code also:
    git push origin source
