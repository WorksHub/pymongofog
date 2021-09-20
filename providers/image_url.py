from faker.providers import BaseProvider

class SafeImageUrl(BaseProvider):
    """
    A Faker provider for Image URLs.
    """

    image_placeholder_services = (
        'https://picsum.photos/{width}/{height}',
        'https://dummyimage.com/{width}x{height}',
        'https://placekitten.com/{width}/{height}',
        'https://placeimg.com/{width}/{height}/any',
    )

    def safe_image_url(self, width=None, height=None):
        """
        Returns URL to placeholder image
        Example: http://placehold.it/640x480
        """
        width_ = width or self.random_int(max=1024)
        height_ = height or self.random_int(max=1024)
        placeholder_url = self.random_element(self.image_placeholder_services)
        return placeholder_url.format(width=width_, height=height_)