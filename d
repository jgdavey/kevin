#!/bin/bash

set -e

template_dir="resources/templates/"
branch="$(basename `git symbolic-ref HEAD`)"

git co design
middleman build
git co $branch

rm -Rf resources/public/*
cp -R build/* resources/public

mv resources/public/*.html $template_dir
