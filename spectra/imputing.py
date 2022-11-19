"""Imputting of missing values in interpolated spectra"""
from configparser import ConfigParser, ExtendedInterpolation
import time
from matplotlib.pyplot import grid

import numpy as np
import pandas as pd

from sdss.process import inputting
from sdss.utils.configfile import ConfigurationFile


start_time = time.perf_counter()

parser = ConfigParser(interpolation=ExtendedInterpolation())
name_config_file = "imputing.ini"
parser.read(f"{name_config_file}")

# A load data frame with meta data
data_directory = parser.get("directory", "data")

spectra_df_name = parser.get("files", "spectra_df")
spectra_df = pd.read_csv(
    f"{data_directory}/{spectra_df_name}", index_col="specobjid"
)

print("Load spectra and objectids")

spectra_file_name = parser.get("files", "spectra")
spectra = np.load(f"{data_directory}/{spectra_file_name}")

# Load indexes and specobjid of interpolated spectra
ids_file_name = parser.get("files", "ids")
track_indexes = np.load(f"{data_directory}/{ids_file_name}")

variance_file_name = parser.get("files", "variance")
variance_of_spectra = np.load(f"{data_directory}/{variance_file_name}")

# mask spectra of science df
specobjids = spectra_df.index.to_numpy()

mask_spectra = np.isin(track_indexes[:, 1], specobjids, assume_unique=True)

spectra = spectra[mask_spectra]
track_indexes = track_indexes[mask_spectra]
variance_of_spectra = variance_of_spectra[mask_spectra]

print("Remove spectra with many indefinite values", end="\n")
number_indefinite_values = np.count_nonzero(~np.isfinite(spectra))

print(f"Indefinite fluxes before drop: {number_indefinite_values}")
drop_fraction_spectra = parser.getfloat("processing", "drop_spectra")

keep_spectra_mask = inputting.drop_spectra(
    spectra=spectra, drop_fraction=drop_fraction_spectra
)

spectra = spectra[keep_spectra_mask]

specobjids = track_indexes[keep_spectra_mask, 1]
indexes = np.arange(0, specobjids.size, 1)
track_indexes = np.hstack((indexes, specobjids))
np.save(f"{data_directory}/ids_inputting.npy", track_indexes)

variance_of_spectra = variance_of_spectra[keep_spectra_mask]

# update meta data with remaining galaxies
spectra_df = spectra_df.loc[specobjids]
spectra_df.to_csv(f"{data_directory}/drop_{spectra_df_name}")

number_indefinite_values = np.count_nonzero(~np.isfinite(spectra))
print(f"Indefinite fluxes after drop: {number_indefinite_values}")

print("Remove wavelegths with many indefinite values", end="\n")

drop_fraction_waves = parser.getfloat("processing", "drop_waves")

keep_waves_mask = inputting.drop_waves(
    spectra=spectra, drop_fraction=drop_fraction_waves
)

spectra = spectra[:, keep_waves_mask]

# Save variance of spectra after indefinite values removal
variance_of_spectra = variance_of_spectra[:, keep_waves_mask]
np.save(
    f"{data_directory}/inputting_variance_spectra.npy", variance_of_spectra
)

number_indefinite_values = np.count_nonzero(~np.isfinite(spectra))

print(f"Indefinite values after drop: {number_indefinite_values}")

print(f"Set new wavelength grid")

interpolation_config_file = parser.get("files", "interpolation_config")
interpolation_parser = ConfigParser(interpolation=ExtendedInterpolation())
interpolation_parser.read(interpolation_config_file)

config = ConfigurationFile()

grid_parametes = config.section_to_dictionary(
    interpolation_parser.items("grid"), value_separators=[" "]
)

wave = np.linspace(
    grid_parametes["lower"],
    grid_parametes["upper"],
    grid_parametes["number_waves"],
)

wave = wave[keep_waves_mask]

np.save(f"{data_directory}/wave.npy", wave)

print("Inputting indefinite values by the median", end="\n")
print("Normalize by the median", end="\n")

nan_median = np.nanmedian(spectra, axis=1)
spectra *= 1 / nan_median.reshape(-1, 1)

indefinite_values_mask = ~np.isfinite(spectra)
spectra[indefinite_values_mask] = 1.0

np.save(f"{data_directory}/spectra.npy", spectra.astype(np.float32))

print("Save configuration file", end="\n")

with open(f"{data_directory}/{name_config_file}", "w") as configfile:
    parser.write(configfile)

finish_time = time.perf_counter()
print(f"Running time: {finish_time - start_time:.2f} [s]")
