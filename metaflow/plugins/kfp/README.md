### Commands used and References

##### Updated Commands (July 17th):

**To run from the terminal**:
```
python 00-helloworld/hello.py run-on-kfp --experiment-name "MF-on-KFP-P2" --run-name "hello_run" --code-url https://raw.githubusercontent.com/zillow/metaflow/mf-on-kfp-2/metaflow/tutorials/00-helloworld/hello.py
```

**To generate just the pipeline YAML file**:
```
python 00-helloworld/hello.py generate-kfp-yaml --code-url https://raw.githubusercontent.com/zillow/metaflow/mf-on-kfp-2/metaflow/tutorials/00-helloworld/hello.py
```


##### Commands used for local orchestration:

For example: To run *pre-start* followed by *start* step:

```
python 00-helloworld/hello.py pre-start | awk 'END{print "python 00-helloworld/hello.py --datastore-root ", $1, " step ", $3, "--run-id", $2, "--task-id", $4, "--input-paths", $2"/"$5"/"$6}' | sh
```

For example: To run *pre-start* followed by *start* step followed by the next step:
```
python 00-helloworld/hello.py pre-start | awk 'END{print "python 00-helloworld/hello.py --datastore-root ", $1, " step ", $3, "--run-id", $2, "--task-id", $4, "--input-paths", $2"/"$5"/"$6}' | sh | awk 'END{print "python 00-helloworld/hello.py --datastore-root ", $1, " step ", $3, "--run-id", $2, "--task-id", $4, "--input-paths", $2"/"$5"/"$6}' | sh
```

##### What's happening inside the step_container_op:

We execute the above local orchestration commands after performing the necessary setup. The current setup includes the following:

- Download the script to be run (needed as we aren't solving code packaging yet)
- Install the modified metaflow version (from Zillow's fork of Metaflow where we are pushing our changes)
- Set a KFP user
- Run the step command