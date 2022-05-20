# Synthetic Data Streams

This directory contains reference configurations for several publicly available datasets that are widely for evaluating 
the performance of algorithms dealing with dataset shift.

## Datasets

The following synthetic datasets are currently available:
- Sine1, Sine2, Mixed, Stagger, Circles, LED [[Link]](https://github.com/alipsgh/data-streams)  
- SEA, Hyperplane [[Link]](https://www.win.tue.nl/~mpechen/data/DriftSets/)

(adding other datasets will be simple based on the available reference configuration)

## Getting started

Follow these steps to produce a `popmon` report for a dataset:

- Download the dataset from the URL above
- Store the dataset in `data/`, and extract if compressed
- Run the relevant reference configurations in this folder (e.g. `led.py`)
- The HTML report will be generated in `reports/`