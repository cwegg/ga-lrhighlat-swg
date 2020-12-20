#!/usr/bin/env python3
import sys
import os.path

for filein in sys.argv[1:]:
    filein = os.path.abspath(filein) # Job submission doesnt play nice with relative paths so resolve
    xml_name = os.path.basename(filein)
    output_dir = os.path.dirname(filein)
    field_name, extension = os.path.splitext(xml_name)
    output_path = os.path.join(output_dir,field_name)
    output_file = f'{output_path}_configured.xml'
    command = f'/soft/configure/configure --gui 0  --threads 8 --field {filein} --output {output_file}'
    qsub_command = f'echo "{command}" | qsub -l pmem=1gb -l walltime=12:00:00 -l nodes=1:ppn=8  -o {output_path}.stdout -e {output_path}.stderr -N {field_name} -'
    print(qsub_command)
    os.system(qsub_command)
