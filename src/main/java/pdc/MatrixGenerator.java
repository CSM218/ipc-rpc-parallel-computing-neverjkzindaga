package pdc;

import java.util.Random;

/**
 * Utility class for generating and manipulating matrices.
 *
 * Provides helper methods for:
 * - Random matrix generation
 * - Identity matrices
 * - Constant-filled matrices
 * - Debug printing
 *
 * NOTE:
 * This class is stateless and thread-safe (Random used only locally).
 */
public final class MatrixGenerator {

    private MatrixGenerator() {
        // Prevent instantiation
    }

    /**
     * Generates a random matrix of specified dimensions.
     *
     * @param rows     number of rows
     * @param cols     number of columns
     * @param maxValue exclusive upper bound for values
     * @return randomly generated matrix
     */
    public static int[][] generateRandomMatrix(int rows, int cols, int maxValue) {

        if (rows <= 0 || cols <= 0) {
            throw new IllegalArgumentException("Matrix dimensions must be positive.");
        }

        if (maxValue <= 0) {
            throw new IllegalArgumentException("maxValue must be positive.");
        }

        Random random = new Random(); // local instance (thread-safe usage)

        int[][] matrix = new int[rows][cols];

        for (int i = 0; i < rows; i++) {
            int[] row = matrix[i];
            for (int j = 0; j < cols; j++) {
                row[j] = random.nextInt(maxValue);
            }
        }

        return matrix;
    }

    /**
     * Generates an identity matrix of specified size.
     *
     * @param size dimension of square matrix
     * @return identity matrix
     */
    public static int[][] generateIdentityMatrix(int size) {

        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive.");
        }

        int[][] matrix = new int[size][size];

        for (int i = 0; i < size; i++) {
            matrix[i][i] = 1;
        }

        return matrix;
    }

    /**
     * Generates a matrix filled with a constant value.
     *
     * @param rows  number of rows
     * @param cols  number of columns
     * @param value fill value
     * @return filled matrix
     */
    public static int[][] generateFilledMatrix(int rows, int cols, int value) {

        if (rows <= 0 || cols <= 0) {
            throw new IllegalArgumentException("Matrix dimensions must be positive.");
        }

        int[][] matrix = new int[rows][cols];

        for (int i = 0; i < rows; i++) {
            int[] row = matrix[i];
            for (int j = 0; j < cols; j++) {
                row[j] = value;
            }
        }

        return matrix;
    }

    /**
     * Prints matrix with optional label.
     */
    public static void printMatrix(int[][] matrix, String label) {

        if (matrix == null) {
            System.out.println("Matrix is null.");
            return;
        }

        if (label != null && !label.isEmpty()) {
            System.out.println(label);
        }

        for (int[] row : matrix) {
            for (int value : row) {
                System.out.printf("%6d ", value);
            }
            System.out.println();
        }
    }

    /**
     * Prints matrix without label.
     */
    public static void printMatrix(int[][] matrix) {
        printMatrix(matrix, null);
    }
}
