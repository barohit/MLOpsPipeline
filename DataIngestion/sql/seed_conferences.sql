INSERT INTO conferences (conference_name, short_name)
VALUES
    ('Big East', 'BIGEAST'),
    ('Big Ten', 'B1G'),
    ('SEC', 'SEC'),
    ('ACC', 'ACC'),
    ('Big 12', 'BIG12')
ON CONFLICT (conference_name) DO NOTHING;
