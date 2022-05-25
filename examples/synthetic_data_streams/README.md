# Synthetic Data Streams

This directory contains reference configurations for several publicly available datasets that are widely for evaluating 
the performance of algorithms dealing with dataset shift.

## Datasets

The following synthetic datasets are currently available:
- Sine1, Sine2, Mixed, Stagger, Circles, LED [[Link]](https://github.com/alipsgh/data-streams)  
- SEA, Hyperplane [[Link]](https://www.win.tue.nl/~mpechen/data/DriftSets/)

| Dataset    | # Instances | # Features | # Classes | Drift type           |
|------------|-------------|------------|-----------|----------------------|
| Sine1      | 100.000     | 2          | 2         | Sudden               |
| Sine2      | 100.000     | 2          | 2         | Sudden               |
| Mixed      | 100.000     | 4          | 2         | Sudden               |
| Stagger    | 100.000     | 3          | 2         | Sudden               |
| Circles    | 100.000     | 2          | 2         | Gradual              |
| LED        | 100.000     | 24         | 10        | Sudden               |
| SEA        | 50.000      | 3          | 2         | Sudden               |
| Hyperplane | 10.000      | 10         | 2         | Gradual; Incremental |

_Characteristics of datasets used, see the survey [Learning under Concept Drift: A Review](https://arxiv.org/pdf/2004.05785.pdf) for more information._

For the sudden-drift datasets, the drifting point is centred at every 5th of the instances for Sine1, Sine2 and Mixed and at each 3rd for Stagger, for a transition over 50 samples. 
For the remaining gradually shifting datasets, Circles and LED, the drifting point is centred around every 4th, and takes place over 500 instances. 
A noise level of 10\% is added to each dataset. 
 where the drifting points occur at each 4th. 
The shift in The Hyperplane dataset that was used, consists of 10.000 samples, and the drift is incremental and gradual.

(adding other datasets will be simple based on the available reference configuration)

## Getting started

Follow these steps to produce a `popmon` report for a dataset:

- Download the dataset from the URL above
- Store the dataset in `data/`, and extract if compressed
- Run the relevant reference configurations in this folder (e.g. `led.py`)
- The HTML report will be generated in `reports/`