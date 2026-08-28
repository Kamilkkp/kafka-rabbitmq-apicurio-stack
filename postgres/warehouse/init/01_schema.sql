CREATE TABLE public.warehouses (
  id uuid PRIMARY KEY,
  code text NOT NULL UNIQUE,
  city text NOT NULL
);

CREATE TABLE public.products (
  id uuid PRIMARY KEY,
  sku text NOT NULL UNIQUE,
  name text NOT NULL,
  unit text NOT NULL
);

CREATE TABLE public.stock_levels (
  warehouse_id uuid NOT NULL REFERENCES public.warehouses (id),
  product_id uuid NOT NULL REFERENCES public.products (id),
  quantity integer NOT NULL,
  PRIMARY KEY (warehouse_id, product_id)
);
