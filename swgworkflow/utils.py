import numpy as np
from astropy.io import fits
from astropy import units as u
from astropy.coordinates import SkyCoord


def fake_catalogue(field_list, filename=None, template='cataloguetemplates/Master_CatalogueTemplate.fits',
                  sources_per_field=[10.0], priorities=[10.0], targuse='T', targsrvy='WL', field_radius=1.0,
                  pseudogrid=False, shot_noise=True):
    import numpy.random as default_rng
    if pseudogrid:
        import sobol_seq
    row = fits.open(template)[1].data
    ra = np.array([])
    dec = np.array([])
    targprio = np.array([])

    for field in field_list:
        field_center = SkyCoord(field['RA'] * u.deg, field['DEC'] * u.deg, frame='icrs')
        for mean_sources, priority in zip(sources_per_field, priorities):
            if shot_noise:
                nsources = default_rng.poisson(mean_sources, 1)
            else:
                nsources = mean_sources

            if pseudogrid:
                random_numbers = sobol_seq.i4_sobol_generate(2, int(nsources), skip=2 * len(ra))
            else:
                random_numbers = default_rng.random((int(nsources), 2))
            position_angle = 360 * random_numbers[:, 0] * u.deg
            distance = field_radius * np.sqrt(random_numbers[:, 1]) * u.deg
            source_positions = field_center.directional_offset_by(position_angle, distance)
            ra = np.append(ra, source_positions.ra.deg)
            dec = np.append(dec, source_positions.dec.deg)
            targprio = np.append(targprio, np.full(nsources, priority))
    hdu = fits.BinTableHDU.from_columns(row.columns, nrows=len(ra))
    hdu.data['GAIA_RA'] = ra
    hdu.data['GAIA_DEC'] = dec
    hdu.data['GAIA_EPOCH'] = '2015.5'
    hdu.data['TARGSRVY'] = targsrvy
    hdu.data['TARGPROG'] = 'MOCKOBJECT'
    hdu.data['TARGCAT'] = filename
    hdu.data['TARGID'] = range(len(ra))
    hdu.data['TARGPRIO'] = targprio
    hdu.data['TARGUSE'] = targuse

    if filename is not None:
        hdu.writeto(filename, overwrite=True)
    else:
        return hdu