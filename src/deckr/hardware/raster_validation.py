"""Semantic validation of the image carried by a raster command."""

import base64
from io import BytesIO

from PIL import Image


def raster_image_fields(params) -> dict[str, object]:
    """Decode a frame and return its actual, rather than asserted, dimensions."""
    data = base64.b64decode(params.image, validate=True)
    try:
        with Image.open(BytesIO(data), formats=("PNG", "JPEG")) as image:
            encoding = image.format.lower()
            if encoding != params.encoding:
                raise ValueError("raster image encoding disagrees with encoding")
            width, height = image.size
            if params.width is not None and params.width != width:
                raise ValueError("raster image width disagrees with width")
            if params.height is not None and params.height != height:
                raise ValueError("raster image height disagrees with height")
            image.verify()
        # Header/CRC validation alone does not detect truncated pixel data.
        with Image.open(BytesIO(data), formats=("PNG", "JPEG")) as image:
            image.load()
    except (OSError, SyntaxError, Image.DecompressionBombError) as exc:
        raise ValueError("raster image cannot be decoded") from exc
    return {"width": width, "height": height, "encoding": encoding}
