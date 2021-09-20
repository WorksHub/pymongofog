# pymongofog
A Python library for 'fogging' a MongoDB database

## Example Config

``` yaml
transform:
  my_db:
    users:
      email: ascii_safe_email
      name:  name
      image_url: safe_image_url
      location:
        city: city
        country: country
      ssn: delete

filters:
  my_db:
    users:
      email: {'$not': {'$regex':example\.com$'}}

```

In this case, records in 'my_db.users' will have a random email, generated
from `faker.ascii_safe_email` applied to the `email` field. Same for `name`,
`image_url`, and also nested values such as `location.city`. The `ssn` field
will be deleted.

The `filters` field defines how the records in the `users` collection will be
filtered. In this case, it will only apply to records with an `email` field that
does not (`$not`) match the provided regular expression (`$regex`).
