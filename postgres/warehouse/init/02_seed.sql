INSERT INTO public.warehouses (id, code, city) VALUES
  ('33333333-3333-4333-8333-333333333333', 'AMS-1', 'Amsterdam'),
  ('44444444-4444-4444-8444-444444444444', 'KRK-1', 'Krakow');

INSERT INTO public.products (id, sku, name, unit) VALUES
  ('55555555-5555-4555-8555-555555555555', 'SKU-MUG', 'Ceramic mug', 'piece'),
  ('66666666-6666-4666-8666-666666666666', 'SKU-TEE', 'Cotton tee', 'piece');

INSERT INTO public.stock_levels (warehouse_id, product_id, quantity) VALUES
  (
    '33333333-3333-4333-8333-333333333333',
    '55555555-5555-4555-8555-555555555555',
    40
  ),
  (
    '44444444-4444-4444-8444-444444444444',
    '55555555-5555-4555-8555-555555555555',
    12
  );
