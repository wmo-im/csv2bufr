#!/bin/bash
apt-get update
apt-get -y install git rsync python3-sphinx python3-sphinx-rtd-theme graphviz texlive
apt-get -y install texlive-formats-extra texlive-xetex dvipng dvisvgm
apt-get -y install texlive-latex-extra
pip install sphinx-rtd-theme sphinx-autodoc-typehints sphinx-autoapi