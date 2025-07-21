#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace EloqStoreBM
{
// struct random_data;
// struct config_weight_list;

class random_generator
{
public:
    random_generator();
    unsigned long long get_random();
    unsigned long long get_random_max() const;
    void set_seed(int seed);

private:
#ifdef HAVE_RANDOM_R
    struct random_data m_data_blob;
    char m_state_array[512];
#elif (defined HAVE_DRAND48)
    unsigned short m_data_blob[3];
#endif
};

class gaussian_noise : public random_generator
{
public:
    gaussian_noise()
    {
        m_hasSpare = false;
    }
    unsigned long long gaussian_distribution_range(double stddev,
                                                   double median,
                                                   unsigned long long min,
                                                   unsigned long long max);

private:
    double gaussian_distribution(const double &stddev);
    bool m_hasSpare;
    double m_spare;
};

#define OBJECT_GENERATOR_KEY_ITERATORS 2 /* number of iterators */
#define OBJECT_GENERATOR_KEY_SET_ITER 1
#define OBJECT_GENERATOR_KEY_GET_ITER 0
#define OBJECT_GENERATOR_KEY_RANDOM -1
#define OBJECT_GENERATOR_KEY_GAUSSIAN -2

class object_generator
{
public:
    enum data_size_type
    {
        data_size_unknown,
        data_size_fixed,
        data_size_range,
        data_size_weighted
    };

protected:
    unsigned int m_key_size;
    unsigned int m_magic_key_size;

    data_size_type m_data_size_type;
    union
    {
        unsigned int size_fixed;
        struct
        {
            unsigned int size_min;
            unsigned int size_max;
        } size_range;
        // config_weight_list *size_list;
    } m_data_size;
    const char *m_data_size_pattern;
    bool m_random_data;
    unsigned int m_expiry_min;
    unsigned int m_expiry_max;
    const char *m_key_prefix;
    unsigned int m_key_prefix_len;
    unsigned long long m_key_min;
    unsigned long long m_key_max;
    double m_key_stddev;
    double m_key_median;

    std::vector<unsigned long long> m_next_key;

    unsigned long long m_key_index;
    char m_key_buffer[250];
    const char *m_key;
    int m_key_len;
    char *m_value_buffer;
    int m_random_fd;
    gaussian_noise m_random;
    unsigned int m_value_buffer_size;
    unsigned int m_value_buffer_mutation_pos;
    std::string m_key_format{"%s%0"};

    bool alloc_value_buffer(void);
    void alloc_value_buffer(const char *copy_from);
    void random_init(void);

public:
    explicit object_generator(
        size_t n_key_iterators = OBJECT_GENERATOR_KEY_ITERATORS);
    object_generator(bool random_data,
                     unsigned int key_size,
                     unsigned int size,
                     const char *key_prefix,
                     unsigned long long key_min,
                     unsigned long long key_max,
                     size_t n_key_iterators = OBJECT_GENERATOR_KEY_ITERATORS);
    object_generator(const object_generator &copy) = delete;
    object_generator(object_generator &&rhs);
    virtual ~object_generator();
    virtual object_generator *clone(void);

    unsigned long long random_range(unsigned long long r_min,
                                    unsigned long long r_max);
    unsigned long long normal_distribution(unsigned long long r_min,
                                           unsigned long long r_max,
                                           double r_stddev,
                                           double r_median);

    void set_random_data(bool random_data);
    void set_key_size(unsigned int size);
    void set_data_size_fixed(unsigned int size);
    void set_data_size_range(unsigned int size_min, unsigned int size_max);
    // void set_data_size_list(config_weight_list *data_size_list);
    void set_data_size_pattern(const char *pattern);
    void set_expiry_range(unsigned int expiry_min, unsigned int expiry_max);
    void set_key_prefix(const char *key_prefix);
    void set_key_range(unsigned long long key_min, unsigned long long key_max);
    void set_key_distribution(double key_stddev, double key_median);
    void set_random_seed(int seed);
    unsigned long long get_key_index(int iter);
    void generate_key(unsigned long long key_index);
    void generate_key(uint64_t key_index, std::string &dest);
    const char *get_key()
    {
        return m_key;
    }
    int get_key_len()
    {
        return m_key_len;
    }

    const char *get_key_prefix();
    virtual const char *get_value(unsigned long long key_index,
                                  unsigned int *len);
    virtual unsigned int get_expiry();
};
}  // namespace EloqStoreBM
