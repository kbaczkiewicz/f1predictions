# F1 driver's predictions application - machine learning project

This is a project that evaluates my ~1 year of studying AI and machine learning algorithms.
It is a document, in form of Jupyter Notebook, that provides comparison of different ML algorithms, both for regression and classification.

## Instructions on running project

### Prerequisities
First, you need to have working Docker on your machine:
https://docs.docker.com/engine/install/

Also, check that you installed Docker-Compose as well:
https://docs.docker.com/compose/install/

### Running guide
Clone this repo on your computer. Then, via terminal or powershell, move to project's catalog and type

```
docker compose up -d
```

After a moment, you should be able to connect to Jupyter notebook.

It lies under address http://localhost:8888

From launcher's view list on the left of the screen, select `F1 prediction.ipynb`.

Execute every code cell one by one - especially when running step below `Import all required libraries` and `Extract data`.
Note that the second step takes a while - it extracts, transforms and loads the data into PostgreSQL database, and creates materialized views that serves data for ML algorithms.

## Contributing
Feel free to send a pull request if you have any ideas or issues with code in this project.

## About
Project created by Kamil Bączkiewicz