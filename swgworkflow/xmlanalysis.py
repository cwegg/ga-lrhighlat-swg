import pandas as pd
import numpy as np
import xml.etree.ElementTree as et
import matplotlib.pyplot as plt
import glob
import logging

PLATE_A_FIBRES, PLATE_B_FIBRES = 964, 948

def parse_xml(xml_file, df_cols, tag='target'):
    """Parse the input XML file and store the result in a pandas
    DataFrame with the given columns.

    The first element of df_cols is supposed to be the identifier
    variable, which is an attribute of each node element in the
    XML data; other features will be parsed from the text content
    of each sub-element.
    """

    xtree = et.parse(xml_file)
    xroot = xtree.getroot()
    rows = []

    for node in xroot.iter(tag):
        res = []
        res.append(node.attrib.get(df_cols[0]))
        for el in df_cols[1:]:
            if node is not None and node.find(el) is not None:
                res.append(node.find(el).text)
            elif node is not None and node.attrib.get(el):
                res.append(node.attrib.get(el))
            else:
                res.append(None)
        rows.append({df_cols[i]: res[i] for i, _ in enumerate(df_cols)})

    out_df = pd.DataFrame(rows, columns=df_cols)

    return out_df


def parse_configure_xml_targets(xml_file, attributes=('targsrvy', 'targra', 'targdec',
                                                      'targuse', 'targclass', 'fibreid',
                                                      'configid', 'targx', 'targy', 'targprio',
                                                      'targprog', 'targid'), add_name=True):
    """Parse an OB xml file on a target-by-target basis and store the result in a pandas
    DataFrame with these columns.

    :param xml_file: the xml file to be parsed
    :param attributes: the attributtes to be extracted
    :param add_name: whether each target should also have the field name
    :return: a pandas dataframe of the extracted data
    """

    df = parse_xml(xml_file, df_cols=attributes)
    df['assigned'] = ~df.fibreid.isnull()

    # We handle the sky fibres separately by setting the TARGSRVY to SKY or AUTOSKY (depending on if they were
    # automatically selected by configure or came from a sky fibre position in an input catalogue) since they
    # really shouldn't be assigned to particular survey...
    if 'targsrvy' in attributes:
        #df.loc[~(df['targsrvy'].isnull() | (df['targsrvy'] == '')) & (df[
        # 'targuse'] == 'S'), 'targsrvy'] = 'SKY'
        df.loc[(df['targsrvy'].isnull() | (df['targsrvy'] == '')) & (df['targuse'] == 'S'), 'targsrvy'] = 'AUTOSKY'

    df.loc[df['targprio'].isnull(), 'targprio'] = -1  # Given to autosky targets to avoid pandas errors later

    # Add the name of the OB to each target for bookkeeping
    if add_name:
        xtree = et.parse(xml_file)
        xroot = xtree.getroot()
        field_name = xroot.find('observation').get('name')
        df['field_name'] = field_name

    # The columns that should be floats need explicit conversion
    float_columns = ('targx', 'targy', 'targra', 'targdec', 'targprio', 'fibreid', 'configid')
    for field in float_columns:
        if field in attributes:
            df[field] = df[field].astype(float)
    return df


def parse_configure_xml_summary(xml_file: str) -> pd.DataFrame:
    """
    Parse an WEAVE OB produced by configure for key information like how many fibres where configured and the hour
    angle range

    :param xml_file: the xml file to parse
    :return: a pandas dataframe containing key information
    """

    row = {}
    # First count the targets that were assigned to each TARGSRVY (e.g. guide/wd/sky/your survey)
    target_df = parse_configure_xml_targets(xml_file)
    assigned_df = target_df[target_df.assigned == True]
    targsrvy_df = assigned_df.groupby(['targsrvy']).size().to_frame(name='assigned').transpose().reset_index(drop=True)
    targsrvy_df['assigned'] = targsrvy_df.sum(axis='columns')  # Add a column for the total fibres assigned
    row.update(targsrvy_df.to_dict())
    #row['assigned'] = targsrvy_df.sum(axis='columns')  # the total fibres assigned

    # parse the xml and extract the tags we care about by hand
    xtree = et.parse(xml_file)
    xroot = xtree.getroot()

    row['progtemp'] = xroot.find('observation').get('progtemp')
    row['obstemp'] = xroot.find('observation').get('obstemp')
    row['field_name'] = xroot.find('observation').get('name')
    row['ra'] = float(xroot.find('observation/fields/field').get('RA_d'))
    row['dec'] = float(xroot.find('observation/fields/field').get('Dec_d'))

    configure_attributes = ('plate', 'max_sky', 'max_calibration', 'max_guide')
    for attribute in configure_attributes:
        row[attribute] = xroot.find('observation/configure').get(attribute)

    # add the max fibres for each survey
    surveys = parse_xml(xml_file, df_cols=['name', 'max_fibres'], tag='survey')
    for i, survey in surveys.iterrows():
        row['max_' + survey['name']] = survey['max_fibres']

    #range of hour angles for which the configuration is valid
    row['hr_min'] = xroot.find('observation/configure/hour_angle_limits').get('earliest')
    row['hr_max'] = xroot.find('observation/configure/hour_angle_limits').get('latest')

    # estimate how many fibres are parked by subtracting the number assigned from the total fibres for each plate
    if row['plate'] == 'PLATE_A':
        row['parked'] = PLATE_A_FIBRES - row['assigned'][0]
    if row['plate'] == 'PLATE_B':
        row['parked'] = PLATE_B_FIBRES - row['assigned'][0]
    df = pd.DataFrame.from_dict(row)
    for field in ('hr_min', 'hr_max'):
        df[field] = df[field].astype(float)
    # reorder cols to have all TARGSRVY at the end
    first_cols = ['field_name', 'ra', 'dec', 'plate', 'hr_min', 'hr_max', 'assigned', 'parked']
    cols = first_cols + [col for col in df if col not in first_cols]
    df = df[cols]
    return df


def parse_configured_xmls(files):
    summarys = []
    targets = []
    if isinstance(files, str):
        file_list = glob.glob(files)
    else:
        file_list = files
        
    for file in file_list:
        try:
            summarys.append(parse_configure_xml_summary(file))
            targets.append(parse_configure_xml_targets(file))
        except:
            logging.info(f'Problem reading {file}')
    return pd.concat(summarys), pd.concat(targets)


def group_assign_df(df, by=('targsrvy',), number=True, fraction=True):
    by.append('assigned')
    df_out = df.groupby(by).size().reset_index(name='number')
    if fraction:
        df_out['fraction'] = df_out['number'] / df_out.groupby(by[:-1])['number'].transform('sum')
    if not number:
        df_out.drop(columns=['number'])
    return df_out
