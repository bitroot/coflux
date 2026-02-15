defmodule Coflux.Store.Bloom do
  @moduledoc """
  Simple Bloom filter implementation for efficient set membership testing.
  Targets ~5-10% false positive rate with configurable size and hash count.
  """

  import Bitwise

  defstruct bits: <<>>, size: 0, hash_count: 0

  @doc """
  Create a new Bloom filter sized for the expected number of elements.
  Uses optimal hash count for ~5% false positive rate.
  """
  def new(expected_elements \\ 10_000) do
    # Optimal size: m = -n * ln(p) / (ln(2))^2
    # For p=0.05: m ≈ 6.24 * n
    bit_size = max(64, round(expected_elements * 6.24))
    # Round up to nearest byte
    byte_size = div(bit_size + 7, 8)
    bit_size = byte_size * 8

    # Optimal hash count: k = (m/n) * ln(2) ≈ 4.3 for 5% FP rate
    hash_count = max(1, round(bit_size / expected_elements * 0.6931))

    %__MODULE__{
      bits: <<0::size(bit_size)>>,
      size: bit_size,
      hash_count: hash_count
    }
  end

  @doc """
  Add a key to the Bloom filter.
  """
  def add(%__MODULE__{} = bloom, key) when is_binary(key) do
    positions = hash_positions(key, bloom.size, bloom.hash_count)

    bits =
      Enum.reduce(positions, bloom.bits, fn pos, bits ->
        set_bit(bits, pos)
      end)

    %{bloom | bits: bits}
  end

  @doc """
  Check if a key might be in the set. Returns true if possibly present,
  false if definitely not present.
  """
  def member?(%__MODULE__{} = bloom, key) when is_binary(key) do
    positions = hash_positions(key, bloom.size, bloom.hash_count)
    Enum.all?(positions, fn pos -> get_bit(bloom.bits, pos) == 1 end)
  end

  @doc """
  Serialize the Bloom filter to a binary for persistence.
  Format: <<size::32, hash_count::8, bits::binary>>
  """
  def serialize(%__MODULE__{} = bloom) do
    <<bloom.size::unsigned-32, bloom.hash_count::unsigned-8, bloom.bits::binary>>
  end

  @doc """
  Deserialize a Bloom filter from a binary.
  """
  def deserialize(<<size::unsigned-32, hash_count::unsigned-8, bits::binary>>) do
    %__MODULE__{
      bits: bits,
      size: size,
      hash_count: hash_count
    }
  end

  # Generate hash positions using double hashing technique:
  # h_i(key) = (h1(key) + i * h2(key)) mod m
  defp hash_positions(key, size, hash_count) do
    <<h1::unsigned-64, h2::unsigned-64, _::binary>> = :crypto.hash(:sha256, key)

    for i <- 0..(hash_count - 1) do
      rem(h1 + i * h2, size)
    end
  end

  defp set_bit(bits, pos) do
    byte_pos = div(pos, 8)
    bit_offset = rem(pos, 8)
    <<prefix::binary-size(byte_pos), byte, rest::binary>> = bits
    new_byte = byte ||| 1 <<< (7 - bit_offset)
    <<prefix::binary, new_byte, rest::binary>>
  end

  defp get_bit(bits, pos) do
    byte_pos = div(pos, 8)
    bit_offset = rem(pos, 8)
    <<_prefix::binary-size(byte_pos), byte, _rest::binary>> = bits
    byte >>> (7 - bit_offset) &&& 1
  end
end
