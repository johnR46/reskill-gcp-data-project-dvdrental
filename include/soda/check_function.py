def check(config_file: str, checks_path: str, scan_name: str, checks_subpath: str, data_source: str):
    from soda.scan import Scan

    # config_file = f'{project_root}/soda/configuration.yml'
    # checks_path = f'{project_root}/soda/checks'

    print('Running Soda Scan ...')

    if checks_subpath:
        checks_path += f'/{checks_subpath}'

    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()
    print(scan.get_logs_text())

    if result != 0:
        raise ValueError('Soda Scan failed')

    return result
