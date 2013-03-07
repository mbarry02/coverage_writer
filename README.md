coverage_writer
===============

continuously writes data to a coverage at various configurable frequencies

coi system and environment needs to be installed -- depends on coverage model 

example:

./bin/python -m fast

./bin/python coverage_writer.py -p \<path_to_coverage\> -m \<mode\>

./bin/python coverage_writer.py -p \<path_to_coverage\> -m \<mode\> --read_interval \<seconds\> --write_interval \<seconds\> --mem_write_interval \<seconds\>--mem_data_depth \<integer\> --data_factor \<integer\> --disk_path \<path\> --rebuild_percentage \<percentage\>
