#!/bin/bash
###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################
# update and install packages used to build docs.
apt-get update
apt-get -y install git rsync python3-sphinx python3-sphinx-rtd-theme graphviz texlive
apt-get -y install texlive-formats-extra texlive-xetex dvipng dvisvgm
apt-get -y install texlive-latex-extra latexmk
pip install sphinx-rtd-theme sphinx-autodoc-typehints sphinx-autoapi

# Build documentation
# ===================
echo "Cleaning previous version"
make -C ./docs clean # clean previous version
echo "Building html docs"
make -C ./docs html # now build the docs

# setup environment
# =================
git config --global user.email "action@github.com"
git config --global user.name "GitHub Action"

# create temporary directory to work in
working_directory=`mktemp -d`

# copy docs to working directory and cd there
rsync -av "docs/build/html/" "${working_directory}/"

pushd "${working_directory}" # use pushd so we can return to current directory easily

# now set up
git init
git remote add deploy "https://token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"
git checkout -b gh-pages
touch .nojekyll
# add readme
cat > README.md <<EOF
Branch containing documentation for repo.
EOF

# add gh-pages to repo
git add .

# commit new files
git commit -am "update of gh-pages"

# deploy
git push deploy gh-pages --force

# return to start dir
popd

# now exit
exit 0