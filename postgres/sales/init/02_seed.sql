INSERT INTO public.customers (id, email, full_name) VALUES
  ('11111111-1111-4111-8111-111111111111', 'ada@example.test', 'Ada Lovelace'),
  ('22222222-2222-4222-8222-222222222222', 'grace@example.test', 'Grace Hopper');

INSERT INTO public.orders (id, customer_id, status, total_cents) VALUES
  (
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    '11111111-1111-4111-8111-111111111111',
    'paid',
    4999
  );

INSERT INTO public.order_items (id, order_id, sku, quantity, unit_price_cents) VALUES
  (
    'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    'SKU-MUG',
    1,
    4999
  );
