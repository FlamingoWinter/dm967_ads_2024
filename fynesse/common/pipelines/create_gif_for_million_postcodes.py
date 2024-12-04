import glob

import seaborn as sns
from PIL import Image
from matplotlib import pyplot as plt

from fynesse.address import sample_near_beaches, sample_not_near_beaches


def init_create_gif_for_million_postcodes(connection):
    df = sample_near_beaches(connection, 1_000_000)
    not_beach_houses = sample_not_near_beaches(connection, 100_000)

    for distance_range in range(50, 1000, 50):
        min_distance, max_distance = distance_range - 50, distance_range
        filtered_data = \
            df[(df['distance_to_beach'] >= min_distance) & (
                        df['distance_to_beach'] <= max_distance)][
                'price']

        plt.figure(figsize=(10, 6))

        if len(filtered_data) > 1:
            sns.kdeplot(filtered_data, fill=True, color="blue", alpha=0.5, gridsize=2000)
            sns.kdeplot(not_beach_houses['price'], fill=True, color="blue", alpha=0.5,
                        gridsize=2000
                        )
            plt.title(f"Distance to Beach: {min_distance}m - {max_distance}m)")
            plt.xlabel("Price")
            plt.xlim(0, 2_000_000)
            plt.ylabel("Density")
            plt.grid(True)
            plt.ylim(0, 8e-6)
        else:
            plt.text(0.5, 0.5, "No Data", horizontalalignment='center', verticalalignment='center')
        plt.savefig(f"distance_full_{min_distance}_{max_distance}.png")

    image_files = sorted(glob.glob("distance_full*.png"))

    images = [Image.open(img) for img in image_files]

    output_gif_path = "animated_kde.gif"
    images[0].save(output_gif_path, save_all=True, append_images=images[1:], duration=500, loop=0)

    print(f"GIF saved to {output_gif_path}")
