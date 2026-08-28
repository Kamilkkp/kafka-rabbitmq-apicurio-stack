CREATE TABLE public.customers (
  id uuid PRIMARY KEY,
  email text NOT NULL UNIQUE,
  full_name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.orders (
  id uuid PRIMARY KEY,
  customer_id uuid NOT NULL REFERENCES public.customers (id),
  status text NOT NULL,
  total_cents integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.order_items (
  id uuid PRIMARY KEY,
  order_id uuid NOT NULL REFERENCES public.orders (id),
  sku text NOT NULL,
  quantity integer NOT NULL,
  unit_price_cents integer NOT NULL
);
